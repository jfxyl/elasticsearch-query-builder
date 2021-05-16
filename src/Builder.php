<?php

namespace Jfxy\ElasticsearchQuery;

use Closure;
use Exception;

abstract class Builder
{
    public $wheres;

    public $postWheres;

    public $fields;

    public $from;

    public $size;

    public $orders;

    public $aggs;

    public $collapse;

    public $highlight;

    public $raw;

    public $dsl;

    public $scroll;

    public $scrollId;

    public $minimumShouldMatch;

    public $minScore;

    public $highlightConfig = [];

    protected $grammar;

    protected $response;

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
    public function select($fields): self
    {
        $this->fields = is_array($fields) ? $fields : func_get_args();
        return $this;
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @param bool $not
     * @param bool $filter
     * @return $this
     * @throws Exception
     */
    public function where($field, $operator = null, $value = null, $boolean = 'and', $not = false, $filter = false): self
    {
        if (is_array($field)) {
            return $this->addArrayOfWheres($field, $boolean, $not, $filter);
        }
        if ($field instanceof Closure && is_null($operator)) {
            return $this->nestedQuery($field, $boolean, $not, $filter);
        }

        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );

        if (in_array($operator, ['!=', '<>'])) {
            $not = !$not;
        }

        if (is_array($value)) {
            return $this->whereIn($field, $value, $boolean, $not, $filter);
        }

        if (in_array($operator, ['>', '<', '>=', '<='])) {
            $value = [$operator => $value];
            return $this->whereBetween($field, $value, $boolean, $not, $filter);
        }

        $type = 'basic';

        $this->wheres[] = compact(
            'type', 'field', 'operator', 'value', 'boolean', 'not', 'filter'
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
    public function orWhere($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'or');
    }


    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function whereNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'and', true);
    }

    /**
     * @param $field
     * @param null $value
     * @return $this
     * @throws Exception
     */
    public function orWhereNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, 'or', true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function filter($field, $operator = null, $value = null, $boolean = 'and', $not = false): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->where($field, $operator, $value, $boolean, $not, true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function orFilter($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'or');
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function filterNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'and', true);
    }

    /**
     * @param $field
     * @param null $operator
     * @param null $value
     * @return $this
     */
    public function orFilterNot($field, $operator = null, $value = null): self
    {
        [$value, $operator] = $this->prepareValueAndOperator(
            $value, $operator, func_num_args() === 2
        );
        return $this->filter($field, $operator, $value, 'or', true);
    }

    /**
     * 单字段查询
     * @param $field
     * @param null $value
     * @param string $type match|match_phrase|match_phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMatch($field, $value = null, $type = 'match', array $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $this->wheres[] = compact(
            'type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter'
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
    public function orWhereMatch($field, $value = null, $type = 'match', array $appendParams = [], $filter = false): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMatch($field, $value = null, $type = 'match', array $appendParams = [], $filter = false): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMatch($field, $value = null, $type = 'match', array $appendParams = [], $filter = false): self
    {
        return $this->whereMatch($field, $value, $type, $appendParams, 'or', true, $filter);
    }

    /**
     * 多字段查询
     * @param $field
     * @param null $value
     * @param string $type best_fields|most_fields|cross_fields|phrase|phrase_prefix
     * @param array $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        [$type, $matchType] = ['multi_match', $type];
        $this->wheres[] = compact(
            'type', 'field', 'value', 'matchType', 'appendParams', 'boolean', 'not', 'filter'
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
    public function orWhereMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = [], $filter = false): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function whereNotMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = [], $filter = false): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param null $value
     * @param string $type
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotMultiMatch($field, $value = null, $type = 'best_fields', array $appendParams = [], $filter = false): self
    {
        return $this->whereMultiMatch($field, $value, $type, $appendParams, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereIn($field, array $value, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'in';
        $this->wheres[] = compact('type', 'field', 'value', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotIn($field, array $value, $filter = false): self
    {
        return $this->whereIn($field, $value, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereIn($field, array $value, $filter = false): self
    {
        return $this->whereIn($field, $value, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotIn($field, array $value, $filter = false): self
    {
        return $this->whereIn($field, $value, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereBetween($field, array $value, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'between';
        $this->wheres[] = compact('type', 'field', 'value', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function whereNotBetween($field, array $value, $filter = false): self
    {
        return $this->whereBetween($field, $value, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereBetween($field, array $value, $filter = false): self
    {
        return $this->whereBetween($field, $value, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param array $value
     * @return $this
     */
    public function orWhereNotBetween($field, array $value, $filter = false): self
    {
        return $this->whereBetween($field, $value, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereExists($field, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'exists';
        $this->wheres[] = compact('type', 'field', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @return $this
     */
    public function whereNotExists($field, $filter = false): self
    {
        return $this->whereExists($field, 'and', true, $filter);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereExists($field, $filter = false): self
    {
        return $this->whereExists($field, 'or', false, $filter);
    }

    /**
     * @param $field
     * @return $this
     */
    public function orWhereNotExists($field, $filter = false): self
    {
        return $this->whereExists($field, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function wherePrefix($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'prefix';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotPrefix($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWherePrefix($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotPrefix($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->wherePrefix($field, $value, $appendParams, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereWildcard($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'wildcard';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotWildcard($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereWildcard($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotWildcard($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereWildcard($field, $value, $appendParams, 'or', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereRegexp($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'regexp';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotRegexp($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereRegexp($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotRegexp($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereRegexp($field, $value, $appendParams, 'or', true, $filter);
    }


    /**
     * @param $field
     * @param $value
     * @param $appendParams
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereFuzzy($field, $value, $appendParams = [], $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'fuzzy';
        $this->wheres[] = compact('type', 'field', 'value', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function whereNotFuzzy($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'and', true, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereFuzzy($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'or', false, $filter);
    }

    /**
     * @param $field
     * @param $value
     * @param array $appendParams
     * @return $this
     */
    public function orWhereNotFuzzy($field, $value, $appendParams = [], $filter = false): self
    {
        return $this->whereFuzzy($field, $value, $appendParams, 'or', true, $filter);
    }

    /**
     * nested类型字段查询
     * @param $path
     * @param $wheres
     * @param $appendParams
     * @return $this
     * @throws Exception
     */
    public function whereNested($path, $wheres, $appendParams = [], $filter = false): self
    {
        if (!($wheres instanceof Closure) && !is_array($wheres)) {
            throw new Exception('非法参数');
        }
        $type = 'nested';
        $boolean = 'and';
        $not = false;
        $query = $this->newQuery()->where($wheres);
        $this->wheres[] = compact('type', 'path', 'query', 'appendParams', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $where
     * @param string $boolean
     * @param bool $not
     * @param bool $filter
     * @return $this
     */
    public function whereRaw($where, $boolean = 'and', $not = false, $filter = false): self
    {
        $type = 'raw';
        $where = is_string($where) ? json_decode($where, true) : $where;
        $this->wheres[] = compact('type', 'where', 'boolean', 'not', 'filter');
        return $this;
    }

    /**
     * @param $where
     * @return $this
     */
    public function orWhereRaw($where, $filter = false): self
    {
        return $this->whereRaw($where, 'or', false, $filter);
    }

    /**
     * 后置过滤器
     * @param $field
     * @param null $operator
     * @param null $value
     * @param string $boolean
     * @param bool $not
     * @return $this
     * @throws Exception
     */
    public function postWhere($field, $operator = null, $value = null, $boolean = 'and', $not = false, $filter = false): self
    {
        $query = $this->newQuery()->where(...func_get_args());
        $this->postWheres = is_array($this->postWheres) ? $this->postWheres : [];
        array_push($this->postWheres, ...$query->wheres);
        return $this;
    }

    /**
     * @param  mixed $value
     * @param  callable $callback
     * @param  callable|null $default
     * @return mixed|$this
     */
    public function when($value, $callback, $default = null): self
    {
        if ($value) {
            return $callback($this, $value) ?: $this;
        } elseif ($default) {
            return $default($this, $value) ?: $this;
        }
        return $this;
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function collapse(string $field, array $appendParams = []): self
    {
        if (empty($appendParams)) {
            $this->collapse = $field;
        } else {
            $this->collapse = array_merge(['field' => $field], $appendParams);
        }
        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function from(int $value): self
    {
        $this->from = $value;
        return $this;
    }

    /**
     * @param int $value
     * @return $this
     */
    public function size(int $value): self
    {
        $this->size = $value;
        return $this;
    }

    /**
     * @param string $field
     * @param string $sort
     * @return $this
     */
    public function orderBy(string $field, $sort = 'asc'): self
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
    public function highlight(string $field, array $params = []): self
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
    public function highlightConfig(array $config = []): self
    {
        $this->highlightConfig = array_merge($this->highlightConfig, $config);
        return $this;
    }

    /**
     * @param $value
     * @return $this
     */
    public function minimumShouldMatch($value): self
    {
        $this->minimumShouldMatch = $value;
        return $this;
    }

    /**
     * @param $value
     * @return Builder
     */
    public function minScore($value): self
    {
        $this->minScore = $value;
        return $this;
    }

    /**
     * @param string $scroll
     * @return $this
     */
    public function scroll($scroll = '2m'): self
    {
        $this->scroll = $scroll;
        return $this;
    }

    /**
     * @param string $scrollId
     * @return $this
     */
    public function scrollId(string $scrollId): self
    {
        if (empty($this->scroll)) {
            $this->scroll();
        }
        $this->scrollId = $scrollId;
        return $this;
    }

    /**
     * @param string $field
     * @param string $type 常用聚合[terms,histogram,date_histogram,date_range,range,cardinality,avg,sum,min,max,extended_stats...]
     * @param array $appendParams 聚合需要携带的参数，聚合不同参数不同，部分聚合必须传入，比如date_histogram需传入[interval=>day,hour...]
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggs(string $alias, string $type = 'terms', $params = [], ... $subGroups): self
    {
        $aggs = [
            'type' => $type,
            'alias' => $alias,
            'params' => $params,
        ];
        foreach ($subGroups as $subGroup) {
            call_user_func($subGroup, $query = $this->newQuery());
            $aggs['subGroups'][] = $query;
        }
        $this->aggs[] = $aggs;
        return $this;
    }

    /**
     * terms 聚合
     * @param string $field
     * @param array $appendParams 聚合需要携带的参数
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
    public function groupBy(string $field, array $appendParams = [], ... $subGroups): self
    {
        $alias = $field . '_terms';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'terms', $params, ... $subGroups);
    }

    /**
     * date_histogram 聚合
     * @param string $field
     * @param string $interval [year,quarter,month,week,day,hour,minute,second,1.5h,1M...]
     * @param string $format 年月日时分秒的表示方式 [yyyy-MM-dd HH:mm:ss]
     * @param array $appendParams
     * @param mixed ...$subGroups
     * @return $this
     */
    public function dateGroupBy(string $field, string $interval = 'day', string $format = "yyyy-MM-dd", array $appendParams = [], ... $subGroups): self
    {
        $alias = $field . '_date_histogram';
        $params = array_merge([
            'field' => $field,
            'interval' => $interval,
            'format' => $format,
            'min_doc_count' => 0,
        ], $appendParams);
        return $this->aggs($alias, 'date_histogram', $params, ... $subGroups);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function cardinality(string $field, array $appendParams = []): self
    {
        $alias = $field . '_cardinality';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'cardinality', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function avg(string $field, array $appendParams = []): self
    {
        $alias = $field . '_avg';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'avg', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function sum(string $field, array $appendParams = []): self
    {
        $alias = $field . '_sum';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'sum', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function min(string $field, array $appendParams = []): self
    {
        $alias = $field . '_min';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'min', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function max(string $field, array $appendParams = []): self
    {
        $alias = $field . '_max';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'max', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function stats(string $field, array $appendParams = []): self
    {
        $alias = $field . '_stats';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'stats', $params);
    }

    /**
     * @param string $field
     * @param array $appendParams
     * @return $this
     */
    public function extendedStats(string $field, array $appendParams = []): self
    {
        $alias = $field . '_extended_stats';
        $params = array_merge(['field' => $field], $appendParams);
        return $this->aggs($alias, 'extended_stats', $params);
    }

    /**
     * @param array $appendParams
     * [
     *      'size' => 1,
     *      'sort' => ['news_posttime' => ['order' => 'desc']],
     *      '_source' => ['news_title','news_posttime','news_url'],
     *      'highlight' => ['fields' => ['news_title' => new \stdClass(),'news_digest' => ['number_of_fragments' => 0]]]
     * ]
     * @return $this
     */

    /**
     * @param $params
     * @return $this
     */
    public function topHits($params): self
    {
        if (!($params instanceof Closure) && !is_array($params)) {
            throw new \InvalidArgumentException('非法参数');
        }
        if ($params instanceof Closure) {
            call_user_func($params, $query = $this->newQuery());
            $params = $query->dsl();
        }
        return $this->aggs('top_hits', 'top_hits', $params);
    }

    /**
     * 聚合内部进行条件过滤
     * @param string $alias 别名
     * @param callable|array $wheres
     * @param mixed ...$subGroups
     * @return $this
     */
    public function aggsFilter(string $alias, $wheres, ... $subGroups): self
    {
        return $this->aggs($alias, 'filter', $this->newQuery()->where($wheres), ... $subGroups);
    }

    protected function addArrayOfWheres($field, $boolean = 'and', $not = false, $filter = false)
    {
        return $this->nestedQuery(function (self $query) use ($field, $not, $filter) {
            foreach ($field as $key => $value) {
                if (is_numeric($key) && is_array($value)) {
                    $query->where(...$value);
                } else {
                    $query->where($key, '=', $value);
                }
            }
        }, $boolean, $not, $filter);
    }

    protected function nestedQuery(Closure $callback, $boolean = 'and', $not = false, $filter = false): self
    {
        call_user_func($callback, $query = $this->newQuery());
        if (!empty($query->wheres)) {
            $type = 'nestedQuery';
            $this->wheres[] = compact('type', 'query', 'boolean', 'not', 'filter');
        }
        return $this;
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
        } elseif (is_array($value) && !in_array($operator, ['=', '!=', '<>'])) {
            throw new Exception('非法运算符和值组合');
        }
        return [$value, $operator];
    }

    /**
     * @param $dsl
     * @return $this
     */
    public function raw($dsl)
    {
        $this->raw = $dsl;
        return $this;
    }


    /**
     * 返回dsl语句
     * @param string $type
     * @return array|false|string|null
     */
    public function dsl($type = 'array')
    {
        if(!empty($this->raw)){
            $this->dsl = $this->raw;
        }else{
            $this->dsl = $this->grammar->compileComponents($this);
        }
        if (!is_string($this->dsl) && $type == 'json') {
            $this->dsl = json_encode($this->dsl, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
        }
        return $this->dsl;
    }

    /**
     * @return mixed
     */
    public function response()
    {
        return $this->response;
    }

    /**
     * 返回文档总数、列表、聚合
     * @param bool $directReturn
     * @return array|null
     */
    public function get($directReturn = false)
    {
        $this->runQuery();
        if ($directReturn) {
            return $this->response;
        }
        try {
            $list = array_map(function ($hit) {
                return $this->decorate(array_merge([
                    '_index' => $hit['_index'],
                    '_type' => $hit['_type'],
                    '_id' => $hit['_id'],
                    '_score' => $hit['_score']
                ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []));
            }, $this->response['hits']['hits']);
            $data = [
                'total' => $this->response['hits']['total'],
                'list' => $list
            ];
            if (isset($this->response['aggregations'])) {
                $data['aggs'] = $this->response['aggregations'];
                //$data['aggs'] = $this->handleAgg($this,$this->response['aggregations']);
            }
            if (isset($this->response['_scroll_id'])) {
                $data['scroll_id'] = $this->response['_scroll_id'];
            }
            return $data;
        } catch (\Exception $e) {
            throw new Exception('数据解析错误-' . $e->getMessage());
        }
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

        if (!empty($this->collapse)) {
            $collapseField = is_string($this->collapse) ? $this->collapse : $this->collapse['field'];
            $this->cardinality($collapseField);
        }
        $this->runQuery();
        try {
            $original_total = $total = $this->response['hits']['total'];
            if (!empty($this->collapse)) {
                $total = $this->response['aggregations'][$collapseField . '_cardinality']['value'];
            }
            $list = array_map(function ($hit) {
                return $this->decorate(array_merge([
                    '_index' => $hit['_index'],
                    '_type' => $hit['_type'],
                    '_id' => $hit['_id'],
                    '_score' => $hit['_score']
                ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []));
            }, $this->response['hits']['hits']);
            $maxPage = intval(ceil($total / $size));
            $data = [
                'total' => $total,
                'original_total' => $original_total,
                'per_page' => $size,
                'current_page' => $page,
                'last_page' => $maxPage,
                'list' => $list
            ];
            if (isset($this->response['aggregations'])) {
                $data['aggs'] = $this->response['aggregations'];
            }
            return $data;
        } catch (\Exception $e) {
            throw new Exception('数据解析错误-' . $e->getMessage());
        }
    }

    /**
     * @param bool $directReturn
     * @return array|null
     */
    public function first($directReturn = false)
    {
        $this->size(1);
        $this->runQuery();
        if ($directReturn) {
            return $this->response;
        }
        $data = null;
        if (isset($this->response['hits']['hits'][0])) {
            $hit = $this->response['hits']['hits'][0];
            $data = $this->decorate(array_merge([
                '_index' => $hit['_index'],
                '_type' => $hit['_type'],
                '_id' => $hit['_id'],
                '_score' => $hit['_score']
            ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []));
        }
        return $data;
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
     * 聚合结果处理(搁置)
     * 暂时仅支持对【terms,histogram,date_histogram,filter,cardinality,avg,sum,min,max,extended_stats,top_hits】
     * 如需拓展请在子类中重写此方法
     * @param Builder $builder
     * @param $aggsResponse
     * @return array
     */
    protected function handleAgg(Builder $builder, $aggsResponse)
    {
        $result = [];
        if (empty($builder->aggs)) {
            return $aggsResponse;
        }
        foreach ($builder->aggs as $agg) {
            $item = [];
            $key = $agg['alias'];
            if (isset($aggsResponse[$key])) {
                switch ($agg['type']) {
                    case 'terms':
                    case 'histogram':
                    case 'date_histogram':
                    case 'date_range':
                    case 'range':
                        $buckets = $aggsResponse[$key]['buckets'];
                        if (!empty($agg['subGroups'])) {
                            foreach ($agg['subGroups'] as $subGroup) {
                                foreach ($buckets as $k => $bucket) {
                                    $buckets[$k] = array_merge($bucket, $this->handleAgg($subGroup, $bucket));
                                }
                            }
                        }
                        $item = $buckets;
                        break;
                    case 'filter':
                        $item['doc_count'] = $aggsResponse[$key]['doc_count'];
                        if (!empty($agg['subGroups'])) {
                            foreach ($agg['subGroups'] as $subGroup) {
                                $item = array_merge($item, $this->handleAgg($subGroup, $aggsResponse[$key]));
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
                    case 'stats':
                    case 'extended_stats':
                        $item = $aggsResponse[$key];
                        break;
                    case 'top_hits':
                        $item = array_map(function ($hit) {
                            return array_merge([
                                '_index' => $hit['_index'],
                                '_type' => $hit['_type'],
                                '_id' => $hit['_id'],
                                '_score' => $hit['_score']
                            ], $hit['_source'], isset($hit['highlight']) ? ['highlight' => $hit['highlight']] : []);
                        }, $aggsResponse[$key]['hits']['hits']);
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
        if (empty($this->scroll)) {
            $this->response = $this->query();
        } else {
            $this->response = $this->scrollQuery();
        }
        if (is_string($this->response)) {
            $this->response = json_decode($this->response, true);
        }
    }

    /**
     * @param $item
     * @return mixed
     */
    protected function decorate($item)
    {
        return $item;
    }

    abstract public function query();

    abstract public function scrollQuery();
}
