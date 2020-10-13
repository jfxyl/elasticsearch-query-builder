<?php
namespace Jfxy\ElasticSearch;

class Grammar
{

    protected $selectComponents = [
        '_source' => 'fields',
        'collapse' => 'collapse',
        'from' => 'from',
        'size' => 'size',
        'sort' => 'orders',
        'query' => 'wheres',
        'aggs' => 'aggs',
        'highlight' => 'highlight',
    ];

    protected $tophitsComponents = [
        '_source' => 'fields',
        'from' => 'from',
        'size' => 'size',
        'sort' => 'orders',
        'highlight' => 'highlight',
    ];

    protected $operatorMappings = [
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    public function compileComponents(Builder $builder) :array
    {
        $dsl = [];
        foreach($this->selectComponents as $k => $v){
            if(!is_null($builder->$v)){
                $method = 'compile' . ucfirst($v);
                $dsl[$k] = $this->$method($builder);
            }
        }
        return $dsl;
    }

    public function compileFields($builder) :array
    {
        return $builder->fields;
    }

    public function compileWheres($builder) :array
    {
        if(empty($builder->wheres)){
            return ["match_all" => new \stdClass()];
        }
        $whereGroups = $this->wherePriorityGroup($builder->wheres);
        $operation = count($whereGroups) === 1 ? 'must' : 'should';
        $bool = [];
        foreach($whereGroups as $wheres){
            $must = [];
            foreach($wheres as $where){
                if($where['type'] === 'Nested'){
                    $must[] = $this->compileWheres($where['query']);
                }else{
                    $must[] = $this->whereMatch($where);
                }
            }
            if (!empty($must)) {
                $bool['bool'][$operation] = $bool['bool'][$operation] ?? [];
                if($operation == 'should'){
                    if(count($must) === 1){
                        array_push($bool['bool'][$operation],array_shift($must));
                    }else{
                        array_push($bool['bool'][$operation],['bool' => ['must' => $must]]);
                    }
                }else{
                    array_push($bool['bool'][$operation],...$must);
                }
            }
        }
        return $bool;
    }

    public function compileAggs(Builder $builder) :array
    {
        $aggs = [];
        foreach($builder->aggs as $agg){
            if($agg['type'] == 'top_hits'){
                $params = $agg['appendParams'];
                foreach($this->tophitsComponents as $k => $v){
                    if(!is_null($builder->$v)){
                        $method = 'compile' . ucfirst($v);
                        $params[$k] = $this->$method($builder);
                    }
                }
            }elseif($agg['type'] == 'filter'){
                $params = $this->compileWheres($agg['wheres']);
            }else{
                $params = array_merge(['field' => $agg['field']],$agg['appendParams']);
            }
            $aggs[$agg['alias']] = [
                $agg['type'] => $params
            ];
            if(!empty($agg['subGroups'])){
                $aggs[$agg['alias']]['aggs'] = [];
                foreach($agg['subGroups'] as $subGroup){
                    $aggs[$agg['alias']]['aggs'] = array_merge($aggs[$agg['alias']]['aggs'],$this->compileAggs($subGroup));
                }
            }
        }
        return $aggs;
    }

    public function compileOrders($builder) :array
    {
        $orders = [];

        foreach ($builder->orders as $field => $orderItem) {
            $orders[$field] = is_array($orderItem) ? $orderItem : ['order' => $orderItem];
        }

        return $orders;
    }

    public function compileSize($builder)
    {
        return $builder->size;
    }

    public function compileFrom($builder)
    {
        return $builder->from;
    }

    public function compileIndex($builder)
    {
        return $builder->index;
    }

    public function compileType($builder)
    {
        return $builder->type;
    }

    public function compileCollapse($builder)
    {
        if(is_string($builder->collapse)){
            $collapse = ['field' => $builder->collapse];
        } else {
            $collapse = $builder->collapse;
        }
        return $collapse;
    }

    public function compileHighlight($builder)
    {
        $highlight = $builder->highlightConfig;
        foreach($builder->highlight as $field => $params){
            $highlight['fields'][$field] = empty($params) ? new \stdClass() : $params;
        }
        return $highlight;
    }

    public function whereMatch(array $where)
    {
        $term = [];
        switch ($where['type']) {
            case 'basic':
                $term = [$where['match'] => [$where['field'] => $where['value']]];
                break;
            case 'match':
            case 'match_phrase':
            case 'match_phrase_prefix':
                $term = [
                    $where['type'] => [
                        $where['field'] => [
                            'query' => $where['value']
                        ]
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term[$where['type']][$where['field']] = array_merge($where['appendParams'],$term[$where['type']][$where['field']]);
                }
                break;
            case 'multi_match':
                $term = [
                    'multi_match' => [
                        'query' => $where['value'],
                        'type' => $where['matchType'],
                        'fields' => $where['field']
                    ]
                ];
                if(!empty($where['appendParams'])){
                    $term['multi_match'] = array_merge($where['appendParams'],$term['multi_match']);
                }
                break;
            case 'between':
                $term = [];
                foreach($where['value'] as $k => $v){
                    if(is_numeric($k)){
                        if($k == 0){
                            $term['range'][$where['field']][$this->operatorMappings['>=']] = $v;
                        }else{
                            $term['range'][$where['field']][$this->operatorMappings['<=']] = $v;
                        }
                    }else{
                        $term['range'][$where['field']][$this->operatorMappings[$k]] = $v;
                    }
                }
                break;
            case 'in':
                $term = ['terms' => [$where['field'] => $where['value']]];
                break;
            case 'exists':
                $term = ['exists' => ['field' => $where['field']]];
                break;
        }
        if($where['not']){
            $term = ['bool'=>['must_not'=>$term]];
        }
        return $term;
    }

    public function wherePriorityGroup($wheres) :array
    {
        $orIndex = array_keys(array_map(function($where){
            return $where['boolean'];
        },$wheres),'or');
        $initIndex = $lastIndex = 0;
        $group = [];
        foreach($orIndex as $index){
            $items = array_slice($wheres,$initIndex,$index - $initIndex);
            if($items) $group[] = $items;
            $initIndex = $lastIndex = $index;
        }
        $group[] = array_slice($wheres, $lastIndex);
        return $group;
    }
}