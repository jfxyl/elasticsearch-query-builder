<?php
namespace Jfxy\ElasticSearch;

use Closure;
use Exception;

abstract class Builder
{
    public $wheres = [];

    public $fields = null;

    public $from = null;

    public $size = null;

    public $orders = null;

    public $aggs = null;

    public $collapse = null;

    public $highlight = null;

    public $dsl = null;

    public $scroll = null;

    public $scrollId = null;

    public $highlightConfig = [];

    protected $grammar = null;

    protected $response = null;

    protected $operatorMappings = [
        '=' => 'eq',
        '>' => 'gt',
        '>=' => 'gte',
        '<' => 'lt',
        '<=' => 'lte',
    ];

    protected $operators = [
        '=', '>', '<', '>=', '<=', '!=', '<>'
    ];

    public function __construct()
    {
        $this->grammar = new Grammar();
    }

    public static function init()
    {
        return new static();
    }

    /**
     * @param $fields
     * @return $this
     */
    public function select($fields) :self
    {
        $this->fields = is_array($fields) ? $fields : func_get_args();

        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $match
     * @param string $boolean
     * @param bool $not
     * @return $this
     * @throws Exception
     */
    public function where($field, $operator = null, $value = null, $match = 'term', $boolean = 'and',$not = false) :self
    {
        if (is_array($field)) {
            return $this->addArrayOfWheres($field, $boolean, $not);
        }

        if ($field instanceof Closure && is_null($operator)) {
            return $this->whereNested($field, $boolean, $not);
        }

        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        if(is_array($value)){
            if($operator == '='){
                return $this->whereIn($field, $value, $boolean, false);
            } elseif (in_array($operator,['!=','<>'])){
                return $this->whereNotIn($field, $value, $boolean, false);
            }
        }

        if(in_array($operator,['>','<','>=','<='])){
            $value = [$operator => $value];
            return $this->whereBetween($field, $value, $boolean, false);
        }

        if(in_array($operator,['!=','<>'])){
            $not = true;
        }

        $type = 'basic';

        $this->wheres[] = compact(
            'type', 'field', 'operator', 'value', 'match', 'boolean', 'not'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function orWhere($field, $operator = null, $value = null) :self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field,$operator,$value,'term','or');
    }


    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function whereNot($field, $value = null) :self
    {
        if($field instanceof Closure){
            throw new Exception('参数错误');
        }
        return $this->where($field, null, $value,'term','and',true);
    }

    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function orWhereNot($field, $value = null) :self
    {
        if($field instanceof Closure){
            throw new Exception('参数错误');
        }
        return $this->where($field, null, $value,'term','or',true);
    }


    /**
     * 单字段查询
     * @param $field
     * @param null $value
     * @param string $type          match|match_phrase|match_phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMatch($field, $value = null,$type = 'match',array $appendParams = [], $boolean = 'and', $not = false) :self
    {
        $this->wheres[] = compact(
            'type', 'field', 'value', 'appendParams', 'boolean', 'not'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', false);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', true);
    }

    /**
     * 多字段查询
     * @param $field
     * @param null $value
     * @param string $type          best_fields|most_fields|cross_fields|phrase|phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = [], $boolean = 'and', $not = false) :self
    {
        [$type,$matchType] = ['multi_match',$type];
        $this->wheres[] = compact(
            'type', 'field', 'value', 'matchType', 'appendParams', 'boolean', 'not'
        );
        return $this;
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', false);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', true);
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereIn($field, array $value, $boolean = 'and', $not = false) :self
    {
        $type = 'in';
        $this->wheres[] = compact('type', 'field', 'value', 'boolean', 'not');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotIn($field, array $value) :self
    {
        return $this->whereIn($field, $value, 'and', true);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereIn($field, array $value) :self
    {
        return $this->whereIn($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotIn($field, array $value) :self
    {
        return $this->whereNotIn($field, $value,'or');
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereBetween($field, array $value, $boolean = 'and', $not = false) :self
    {
        $type = 'between';
        $this->wheres[] = compact('type','field','value','boolean','not');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotBetween($field, array $value) :self
    {
        return $this->whereBetween($field, $value, 'and', true);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereBetween($field, array $value) :self
    {
        return $this->whereBetween($field, $value, 'or');
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotBetween($field, array $value) :self
    {
        return $this->whereBetween($field, $value, 'or', true);
    }

    /**
     * @param $field
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereExists($field,$boolean = 'and', $not = false) :self
    {
        $type = 'exists';
        $this->wheres[] = compact('type','field','boolean','not');
        return $this;
    }

    /**
     * @param $field
     * @return $this
     */
    public function whereNotExists($field) :self
    {
        return $this->whereExists($field,'and',true);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereExists($field) :self
    {
        return $this->whereExists($field,'or');
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereNotExists($field) :self
    {
        return $this->whereExists($field,'or',true);
    }

    /**
     * @param  mixed  $value
     * @param  callable  $callback
     * @param  callable|null  $default
     * @return mixed|$this
     */
    public function when($value,$callback,$default = null) :self
    {
        if($value){
            return $callback($this,$value)?:$this;
        } elseif ($default) {
            return $default($this,$value)?:$this;
        }
        return $this;
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function collapse(string $field,array $appendParams = []) :self
    {
        if(empty($appendParams)){
            $this->collapse = $field;
        }else{
            $this->collapse = array_merge(['field' => $field],$appendParams);
        }
        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function from(int $value) :self
    {
        $this->from = $value;

        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function size(int $value) :self
    {
        $this->size = $value;

        return $this;
    }

    /**
     * @param string $field
     * @param string $sort
     * @return $this
     */
    public function orderBy(string $field, $sort = 'asc') :self
    {
        $this->orders[$field] = $sort;

        return $this;
    }

    /**
     * 高亮字段
     * @param string $field
     * @param array $params
     * [
     *      "number_of_fragments" => 0  // 字段片段数
     *      ...
     * ]
     * @return $this
     */
    public function highlight(string $field,array $params = [])
    {
        $this->highlight[$field] = $params;
        return $this;
    }

    /**
     * 高亮配置
     * @param array $config
     * [
     *      "require_field_match" => false,     // 是否只高亮查询的字段
     *      "number_of_fragments" => 1,         // 高亮字段会被分段，返回分段的个数，设置0不分段
     *      "pre_tags" => "<em>",
     *      "post_tags" => "</em>",
     *      ...
     * ]
     * @return $this
     */
    public function highlightConfig(array $config = [])
    {
        $this->highlightConfig = array_merge($this->highlightConfig,$config);
        return $this;
    }

    /**
     * @param string $scroll
     * @return $this
     */
    public function scroll($scroll = '2M') :self
    {
        $this->scroll = $scroll;
        return $this;
    }

    /**
     * @param string $scrollId
     * @return $this
     */
    public function scrollId(string $scrollId) :self
    {
        if(empty($this->scroll)){
            $this->scroll();
        }
        $this->scrollId = $scrollId;
        return $this;
    }

    /**
     * @param string $field
     * @param string $type  常用聚合[terms,histogram,date_histogram,date_range,range,cardinality,avg,sum,min,max,extended_stats...]
     * @param array $appendParams  聚合需要携带的参数，聚合不同参数不同，部分聚合必须传入，比如date_histogram需传入[interval=>day,hour...]
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggs(string $field,string $type = 'terms',array $appendParams = [], ... $subGroups) :self
    {
        $aggs = [
            'field' => $field,
            'type' => $type,
            'alias' => $field.'_'.$type,
            'appendParams' => $appendParams,
        ];
        foreach($subGroups as $subGroup){
            call_user_func($subGroup,$query = $this->newQuery());
            $aggs['subGroups'][] = $query;
        }
        $this->aggs[] = $aggs;
        return $this;
    }

    /**
     * terms 聚合
     * @param string $field
     * @param array $appendParams  聚合需要携带的参数
     *      [
     *          'size' => 10,                   // 默认
     *          'order' => ['_count'=>'desc']   // 默认
     *          'order' => ['_count'=>'asc']
     *          'order' => ['_key'=>'desc']
     *          'order' => ['_key'=>'asc']
     *          ...
     *      ]
     * @param mixed ...$subGroups
     * @return $this
     */
    public function groupBy(string $field, array $appendParams = [], ... $subGroups) :self
    {
        return $this->aggs($field,'terms',$appendParams, ... $subGroups);
    }

    /**
     * date_histogram 聚合
     * @param string $field
     * @param string $interval  [year,quarter,month,week,day,hour,minute,second,1.5h,1M...]
     * @param string $format    年月日时分秒的表示方式 [yyyy-MM-dd HH:mm:ss]
     * @param array $appendParams
     * @param mixed ...$subGroups
     * @return $this
     */
    public function dateGroupBy(string $field,string $interval = 'day',string $format = "yyyy-MM-dd",array $appendParams = [], ... $subGroups) :self
    {
        $defaultParams = [
            "interval" => $interval,
            "format" => $format,
            "min_doc_count" => 0,
        ];
        $appendParams = array_merge($defaultParams,$appendParams);
        return $this->aggs($field,'date_histogram',$appendParams, ... $subGroups);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function cardinality(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'cardinality',$appendParams);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function avg(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'avg',$appendParams);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function sum(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'sum',$appendParams);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function min(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'min',$appendParams);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function max(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'max',$appendParams);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function extendedStats(string $field,array $appendParams = []) :self
    {
        return $this->aggs($field,'extended_stats',$appendParams);
    }

    /**
     * 聚合组内返回文档
     * @param array $appendParams
     * [
     *      'size' => 1,
     *      'sort' => ['news_posttime' => ['order' => 'desc']],
     *      '_source' => ['news_title','news_posttime','news_url'],
     *      'highlight' => ['fields' => ['news_title' => new \stdClass(),'news_digest' => ['number_of_fragments' => 0]]]
     * ]
     * @return $this
     */
    public function topHits(array $appendParams = []) :self
    {
        $aggs = [
            'type' => 'top_hits',
            'alias' => 'top_hits',
            'appendParams' => $appendParams,
        ];
        $this->aggs[] = $aggs;
        return $this;
    }

    /**
     * 聚合内部进行条件过滤
     * @param string $alias 别名
     * @param callable|array $wheres
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggsFilter(string $alias,$wheres,... $subGroups) :self
    {
        $aggs = [
            'type' => 'filter',
            'alias' => $alias,
            'wheres' => $this->newQuery()->where($wheres)
        ];
        foreach($subGroups as $subGroup){
            call_user_func($subGroup,$query = $this->newQuery());
            $aggs['subGroups'][] = $query;
        }
        $this->aggs[] = $aggs;
        return $this;
    }

    /**
     * @param $dsl
     * @return $this
     */
    public function raw($dsl)
    {
        $this->dsl = $dsl;
        return $this;
    }


    /**
     * 返回dsl语句
     * @param string $type
     * @return array|false|string|null
     */
    public function dsl($type = 'array')
    {
        if(empty($this->dsl)){
            $this->dsl = $this->grammar->compileComponents($this);
        }
        if(!is_string($this->dsl) && $type == 'json'){
            $this->dsl =  json_encode($this->dsl,JSON_UNESCAPED_UNICODE|JSON_PRETTY_PRINT);
        }
        return $this->dsl;
    }

    /**
     * 返回文档总数、列表、聚合
     * @param bool $directReturn
     * @return array|null
     */
    public function get($directReturn = false)
    {
        $this->runQuery();
        if($directReturn){
            return $this->response;
        }
        $list = array_map(function($hit){
            return array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ],$hit['_source'],isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
        },$this->response['hits']['hits']);

        $data = [
            'total' => $this->response['hits']['total'],
            'list' => $list
        ];
        if(isset($this->response['aggregations'])){
            $data['aggs'] = $this->response['aggregations'];
//            $data['aggs'] = $this->handleAgg($this,$this->response['aggregations']);
        }
        if(isset($this->response['_scroll_id'])){
            $data['scroll_id'] = $this->response['_scroll_id'];
        }
        return $data;
    }

    /**
     * 分页查询
     * @param int $page
     * @param int $size
     * @return array
     */
    public function paginator(int $page = 1, int $size = 10)
    {
        $from = ($page - 1) * $size;

        $this->from($from);

        $this->size($size);

        if(!empty($this->collapse)){
            $collapseField = is_string($this->collapse) ? $this->collapse : $this->collapse['field'];
            $this->aggs($collapseField,'cardinality');
        }

        $this->runQuery();

        if(!empty($this->collapse)){
            $total = $this->response['aggregations'][$collapseField.'_cardinality']['value'];
        }else{
            $total = $this->response['hits']['total'];
        }

        $list = array_map(function($hit){
            return array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ],$hit['_source']);
        },$this->response['hits']['hits']);

        $maxPage = intval(ceil($total / $size));
        return [
            'total' => $total,
            'per_page' => $size,
            'current_page' => $page,
            'last_page' => $maxPage,
            'list' => $list
        ];
    }

    /**
     * @param bool $directReturn
     * @return array|null
     */
    public function first($directReturn = false)
    {
        $this->size(1);
        $this->runQuery();
        if($directReturn){
            return $this->response;
        }
        if(isset($this->response['hits']['hits'][0])){
            $hit = $this->response['hits']['hits'][0];
            $data = array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ],$hit['_source'],isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
        }
        return @$data;
    }

    /**
     * 返回命中的文档个数
     * @return int
     */
    public function count()
    {
        $this->runQuery();
        return $this->response['hits']['total'];
    }

    /**
     * 批量操作
     * @param callable $callback
     * @param int $size
     * @param string $scroll
     */
    public function chunk(callable $callback, int $size = 1000,$scroll = '2M')
    {
        if (empty($this->scroll)) {
            $this->scroll = $scroll;
        }

        if (empty($this->size)) {
            $this->size = $size;
        }

        $this->runQuery();

        $this->scrollId = $this->response['_scroll_id'];

        $total = $this->response['hits']['total'];

        $whileNum = intval(floor($total / $this->size));

        do {
            $data = array_map(function($hit){
                return array_merge([
                    '_index' => $hit['_index'],
                    '_type' => $hit['_type'],
                    '_id' => $hit['_id'],
                    '_score' => $hit['_score']
                ],$hit['_source']);
            },$this->response['hits']['hits']);

            call_user_func($callback,$data);

            $this->runQuery();

        } while ($whileNum--);
    }

    /**
     * 聚合结果处理
     * 暂时仅支持对【terms,histogram,date_histogram,filter,cardinality,avg,sum,min,max,extended_stats,top_hits】
     * 如需拓展请在子类中重写此方法
     * @param Builder $builder
     * @param $aggsResponse
     * @return array
     */
    protected function handleAgg(Builder $builder,$aggsResponse)
    {
        $result = [];
        if(empty($builder->aggs)){
            return $aggsResponse;
        }
        foreach($builder->aggs as $agg){
            $item = [];
            $key = $agg['alias'];
            if(isset($aggsResponse[$key])){
                switch($agg['type']){
                    case 'terms':
                    case 'histogram':
                    case 'date_histogram':
                    case 'date_range':
                    case 'range':
                        $buckets = $aggsResponse[$key]['buckets'];
                        if(!empty($agg['subGroups'])){
                            foreach($agg['subGroups'] as $subGroup){
                                foreach($buckets as $k => $bucket){
                                    $buckets[$k] = array_merge($bucket,$this->handleAgg($subGroup,$bucket));
                                }
                            }
                        }
                        $item = $buckets;
                        break;
                    case 'filter':
                        $item['doc_count'] = $aggsResponse[$key]['doc_count'];
                        if(!empty($agg['subGroups'])){
                            foreach($agg['subGroups'] as $subGroup){
                                $item = array_merge($item,$this->handleAgg($subGroup,$aggsResponse[$key]));
                            }
                        }
                        break;
                    case 'cardinality':
                    case 'avg':
                    case 'sum':
                    case 'min':
                    case 'max':
                        $item = $aggsResponse[$key]['value'];
                        break;
                    case 'extended_stats':
                        $item = $aggsResponse[$key];
                        break;
                    case 'top_hits':
                        $item = array_map(function($hit){
                            return array_merge([
                                '_index' => $hit['_index'],
                                '_type' => $hit['_type'],
                                '_id' => $hit['_id'],
                                '_score' => $hit['_score']
                            ],$hit['_source'],isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
                        },$aggsResponse[$key]['hits']['hits']);
                        break;
                    default:
                        // 太多了，头疼
                        $item = $aggsResponse[$key];
                        break;
                }
            }
            $result[$key] = $item;
        }
        return $result;
    }

    /**
     * 执行查询，并返回结果数组
     * @return mixed
     */
    protected function runQuery()
    {
        $this->dsl('json');
        if(empty($this->scroll)){
            $this->response = $this->query();
        }else{
            $this->response = $this->scrollQuery();
        }
        if(is_string($this->response)){
            $this->response = json_decode($this->response,JSON_UNESCAPED_UNICODE);
        }
    }

    protected function addArrayOfWheres($field, $boolean, $not, $method = 'where')
    {
        return $this->whereNested(function ($query) use ($field, $method, $boolean, $not) {
            foreach ($field as $key => $value) {
                if (is_numeric($key) && is_array($value)) {
                    $query->{$method}(...array_values($value));
                } else {
                    $matchType = is_array($value) ? 'terms' : 'term';
                    $query->$method($key, '=', $value, $matchType, $boolean, $not);
                }
            }
        }, $boolean);
    }

    public function whereNested(Closure $callback, $boolean = 'and',$not = false) :self
    {
        call_user_func($callback, $query = $this->newQuery());
        return $this->addNestedWhereQuery($query, $boolean, $not);
    }

    protected function newQuery()
    {
        return new static();
    }

    protected function prepareValueAndOperator($value, $operator, $useDefault = false)
    {
        if ($useDefault) {
            return [$operator, '='];
        } elseif (is_null($value) && in_array($operator, $this->operators)) {
            throw new Exception('非法运算符和值组合');
        }
        return [$value, $operator];
    }

    protected function invalidOperator($operator)
    {
        return !in_array(strtolower($operator), $this->operators, true);
    }

    protected function addNestedWhereQuery($query, $boolean = 'and', $not = false)
    {
        if (count($query->wheres)) {
            $type = 'Nested';
            $this->wheres[] = compact('type', 'query', 'boolean','not');
        }
        return $this;
    }

    abstract public function query();

    abstract public function scrollQuery();
}
