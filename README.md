# elasticsearch-query-builder

## 安装

```
composer require jfxy/elasticsearch-query-builder
```

## 注意
* elasticsearch <= 6.8
* php >= 7.1
* 需要子类继承**Jfxy\ElasticsearchQuery\Builder**并实现**query()** 和 **scrollQuery()**
* 复杂的业务查询应该在子类中封装
* 下面将子类定义为**Es**

## 方法
#### select
```php
    public function select($fields) :self
    
    ->select('id','name')
    ->select(['id','name'])
```

#### where
* 比较运算符支持 **=,>,>=,<,<=,!=,<>**
* where、orWhere、whereNot、orWhereNot均支持闭包调用，而orWhere、whereNot、orWhereNot则是对闭包内的整体条件进行 or 和 not 的操作，闭包用法类似mysql中对闭包内的条件前后加上（）
* 在封装业务代码存在or关系时，应使用闭包包裹内部条件
```php
    public function where($field, $operator = null, $value = null, $boolean = 'and', $not = false, $filter = false) :self
    public function orWhere($field, $operator = null, $value = null) :self
    public function whereNot($field, $value = null) :self
    public function orWhereNot($field, $value = null) :self
    
    ->where('id',1)
    ->where('id','=',1)
    ->where('id',[1,2])                         // 等同于  ->whereIn('id',[1,2])
    ->where('news_postdate','<=','2020-09-01')  // 等同于  ->whereBetween('news_postdate',['<=' => '2020-09-01'])
    
    // 闭包用法
    ->where(function($query){
        return $query->where('id',1)->orWhere('status','>',0);
    })
    ->orWhere(function($query){
        return $query->where('id',1)->orWhere('status','>',0);
    })
    
    // 数组用法，下面两种写法类似，数组用法下的time条件顺序跟直接传入where方法顺序一致即可
    ->where(['id' => 1,'status' => [0,1],['time','>=','2020-09-01']])
    ->where(function($query){
        $query->where('id',1)->where('status',[0,1])->where('time','>=','2020-09-01');
    })
    
    // whereNot实现 a != 1 and b != 2
    ->whereNot('a',1)->whereNot('b',2)
    
    // whereNot实现 a != 1 or b != 2，即not(a=1 and b=2)
    ->whereNot(['a'=>1,'b'=>2])
    ->whereNot(function($query){
        $query->where('a',1)->where('b',2);
    })
```

#### filter
* 用法同where一致，不过条件会写在filter下
```php
    public function filter($field, $operator = null, $value = null, $boolean = 'and',$not = false) :self
    public function orFilter($field, $operator = null, $value = null) :self
    public function filterNot($field, $value = null) :self
    public function orFilterNot($field, $value = null) :self
```

#### in
```php
    public function whereIn($field, array $value, $boolean = 'and', $not = false) :self
    public function whereNotIn($field, array $value, $boolean = 'and') :self
    public function orWhereIn($field, array $value) :self
    public function orWhereNotIn($field, array $value) :self
    
    ->whereIn('id',[1,2])
```

#### between
* 默认为闭区间，比较运算符支持 **>,>=,<,<=**
```php
    public function whereBetween($field, array $value, $boolean = 'and', $not = false) :self
    public function whereNotBetween($field, array $value, $boolean = 'and') :self
    public function orWhereBetween($field, array $value) :self
    public function orWhereNotBetween($field, array $value) :self
    
    ->whereBetween('id',[1,10])                     // 1 <= id <= 10
    ->whereBetween('id',[1,'<' => 10])              // 1 <= id < 10
    ->whereBetween('id',['>=' => 1,'<' => 10])      // 1 <= id < 10
```

#### exists
* 字段不存在或为null
```php
    public function whereExists($field,$boolean = 'and', $not = false) :self
    public function whereNotExists($field) :self
    public function orWhereExists($field) :self
    public function orWhereNotExists($field) :self
    
    ->whereExists('news_uuid')
```

#### prefix 前缀匹配
```php
    public function wherePrefix($field, $value, $appendParams = [], $boolean = 'and', $not = false) :self
    public function whereNotPrefix($field, $value, $appendParams = []) :self
    public function orWherePrefix($field, $value, $appendParams = []) :self
    public function orWhereNotPrefix($field, $value, $appendParams = []) :self
    
    ->wherePrefix('news_url','http://www.baidu.com')
```

#### wildcard 通配符匹配
```php
    public function whereWildcard($field, $value, $appendParams = [], $boolean = 'and', $not = false) :self
    public function whereNotWildcard($field, $value, $appendParams = []) :self
    public function orWhereWildcard($field, $value, $appendParams = []) :self
    public function orWhereNotWildcard($field, $value, $appendParams = []) :self
    
    ->whereWildcard('media_name','*公安')
```

#### regexp 正则匹配
```php
    public function whereRegexp($field, $value, $appendParams = [], $boolean = 'and', $not = false) :self
    public function whereNotRegexp($field, $value, $appendParams = []) :self
    public function orWhereRegexp($field, $value, $appendParams = []) :self
    public function orWhereNotRegexp($field, $value, $appendParams = []) :self
    
    ->whereRegexp('media_name','.*公安')
```

#### fuzzy 模糊查询
```php
    public function whereFuzzy($field, $value, $appendParams = [], $boolean = 'and', $not = false) :self
    public function whereNotFuzzy($field, $value, $appendParams = []) :self
    public function orWhereFuzzy($field, $value, $appendParams = []) :self
    public function orWhereNotFuzzy($field, $value, $appendParams = []) :self
    
    ->whereFuzzy('news_title','安徽合肥')
```

#### whereRaw 原生条件
```php
    public function whereRaw($where, $boolean = 'and', $not = false) :self
    public function orWhereRaw($where) :self
    
    // 下面的例子是由于where方法提供的term查询无法设置一些其他的参数,可以改为使用whereRaw
    ->whereRaw([
       "term" => [
           "news_title" => [
               "value" => "安徽",
               "boost" => 2
           ]
       ]
   ])
   
   ->whereRaw([
       'bool' => [
           'must' => [
               "term" => [
                   "news_title" => [
                       "value" => "安徽",
                       "boost" => 2
                   ]
               ]
           ]
       ]
   ])
```

#### match
* whereMatch方法，$type=match、match_phrase、match_phrase_prefix   
* whereMultiMatch方法，$type=best_fields、most_fields、cross_fields、phrase、phrase_prefix
```php
    // 单字段
    public function whereMatch($field, $value = null,$type = 'match',array $appendParams = [], $boolean = 'and', $not = false) :self
    public function orWhereMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    public function whereNotMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    public function orWhereNotMatch($field, $value = null,$type = 'match',array $appendParams = []) :self
    // 多字段
    public function whereMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = [], $boolean = 'and', $not = false) :self
    public function orWhereMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    public function whereNotMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    public function orWhereNotMultiMatch($field, $value = null,$type = 'best_fields',array $appendParams = []) :self
    
    ->whereMatch('news_title','上海','match_phrase',['slop'=>1])
    ->whereMultiMatch(['news_title','news_content'],'上海','phrase',["operator" => "OR"])
```

#### minimumShouldMatch 最小匹配度
```php
    public function minimumShouldMatch($value) :self
    
    ->where('aa',1)->orWhere('bb',1)->orWhere('cc',1)->minimumShouldMatch(2)
    
    ->where(function(Es $query){
        $query->where('aa',1)->orWhere('bb',1)->orWhere('cc',1)
            ->minimumShouldMatch('50%');
    })
    
    ->postWhere(function(Es $query){
        $query->where('aa',1)->orWhere('bb',1)->orWhere('cc',1)
            ->minimumShouldMatch('50%');
    })
```

#### whereNested nested类型字段查询
* 仅支持传入闭包和数组条件
```php
    public function whereNested($path,$wheres,$appendParams = []) :self
    
    ->whereNested('skus',function(Es $query){
        $query->where('skus.title','iphone')->where('skus.des','iphone');
    },['inner_hits'=>['highlight' => ['fields'=>['skus.title'=>new \stdClass()]]]]);
    
    ->whereNested('skus',['skus.title' => 'iphone','skus.description' => 'iphone',['skus.price','>','100']],['inner_hits'=>['highlight' => ['fields'=>['skus.title'=>new \stdClass()]]]]);
```

#### postWhere 后置过滤器
* postWhere方法添加的条件会作用于post_filter查询，条件作用于聚合之后
* postWhere方法参数同where方法相同，复杂的检索可以传入数组或闭包
```php
    public function postWhere($field, $operator = null, $value = null, $boolean = 'and',$not = false) :self
    
    ->postWhere('platform','wx')
    ->postWhere(['platform' => ['wx','web'],['news_posttime','>','2020-09-01 00:00:00']])
    ->postWhere(function(Es $query){
        $query->where('platform','wx')->whereNotMatch('news_title','安徽合肥')->orWhereIn('news_postdate',['2020-09-01','2020-09-02']);
    })
```

#### when
* $value为true时会执行$callback，否则当$default存在时会执行$default
```php
    public function when($value,$callback,$default = null) :self
    
    ->when(1 > 2,function($query){
        return $query->whereBetween('news_postdate',['2020-05-01','2020-05-05']);
    },function($query){
        return $query->whereBetween('news_postdate',['2020-05-09','2020-05-10']);
    })
```

#### collapse 折叠
* 使用collapse方法并不会使返回的总数发生变化，计算折叠后的总数需要配合cardinality聚合使用
* collapse方法和paginator方法一起使用时，paginator方法内部会对折叠的字段做cardinality聚合，不需要考虑collapse的总数问题
```php
    public function collapse(string $field,array $appendParams = []) :self
    
    ->collapse('news_sim_hash')
    ->collapse('news_sim_hash')->aggs('alias','cardinality',['field'=>'news_sim_hash'])
    ->collapse('news_sim_hash')->cardinality('news_sim_hash')
    ->collapse('news_sim_hash')->paginator()
```

#### from
```php
    public function from(int $value) :self
```
#### size
```php
    public function size(int $value) :self
```
#### orderBy 排序
```php
    public function orderBy(string $field, $sort = 'asc') :self
    
    ->orderBy('news_posttime','asc')->orderBy('news_like_count','desc')
```
#### highlight 高亮
* 高亮配置及高亮字段  
* 建议先在Es子类中设置highlightConfig通用属性 
```php
    // 根据自己的需要在子类中配置
    public $highlightConfig = [
        "require_field_match" => false,     // 是否只高亮查询的字段
        "number_of_fragments" => 0,         // 高亮字段会被分段，返回分段的个数，设置0不分段
        "pre_tags" => "<em>",
        "post_tags" => "</em>",
    ];
``` 
* 使用highlightConfig方法会覆盖highlightConfig通用属性中的同键名配置  
* highlight方法指定高亮字段并且设置指定字段的高亮属性
```php
    public function highlight(string $field,array $params = [])
    public function highlightConfig(array $config = [])
    
    ->highlightConfig(['require_field_match'=>false,'number_of_fragments' => 0,'pre_tags'=>'<h3>','post_tags'=>'</h3>'])
    ->highlight('news_title')->highlight('news_digest',['number_of_fragments' => 0])
```

#### aggs   聚合
* $alias参数是该聚合的别名
* $type参数是聚合的类型，terms、histogram、date_histogram、date_range、range、cardinality、avg、sum、min、max、extended_stats、top_hits、filter...
* $params参数是不同聚合类型下的条件键值对数组
* ...$subGroups参数是嵌套聚合，通过传递闭包参数调用，可同时传递多个闭包
```php
    public function aggs(string $alias,string $type = 'terms',$params = [], ... $subGroups) :self
    
    ->aggs('alias','terms',['field'=>'platform','size'=>15,'order' => ['_count'=>'asc']])
    ->aggs('alias','date_histogram',['field'=>'news_posttime','interval' => 'day','format' => 'yyyy-MM-dd','min_doc_count' => 0])
    ->aggs('alias','histogram',['field'=>'news_like_count','interval'=>10])
    ->aggs('alias','extended_stats',['field'=>'news_like_count'])
    ->aggs('alias','cardinality',['field'=>'news_sim_hash'])
    ->aggs('alias','avg',['field'=>'news_like_count'])
    ->aggs('alias','sum',['field'=>'news_like_count'])
    ->aggs('alias','min',['field'=>'news_like_count'])
    ->aggs('alias','max',['field'=>'news_like_count'])
    ->aggs('alias','date_range',[
        'field' => 'news_posttime',
        'format'=> 'yyyy-MM-dd',
        'ranges'=>[
            ['from'=>'2020-09-01','to'=>'2020-09-02'],
            ['from'=>"2020-09-02",'to'=>'2020-09-03']
        ]
    ])
    ->aggs('alias','range',[
        'field' => 'media_CI',
        'ranges'=>[
            ['key'=>'0-500','to'=>'500'],
            ['key'=>'500-1000','from'=>'500','to'=>'1000'],
            ['key'=>'1000-∞','from'=>'1000'],
        ]
    ])
    ->aggs('alias','top_hits',$params)
    ->aggs('alias','filter',function(Es $query){
        $query->where('news_posttime','>','2020-09-01 00:00:00');
    })
```

* groupBy方法是aggs的terms类型聚合的封装  
````php
    public function groupBy(string $field, array $appendParams = [], ... $subGroups) :self
    
    ->groupBy('platform',['size'=>20,'order'=>['_count'=>'asc']])
    
    // $appendParams 常用的一些设置，不同的聚合类型参数不同
    $appendParams = [
        'size' => 10,                   // 默认
        'order' => ['_count'=>'desc']   // 默认，文档数量倒序
        'order' => ['_count'=>'asc']    // 文档数量顺序
        'order' => ['_key'=>'desc']     // 分组key倒序
        'order' => ['_key'=>'asc']      // 分组key顺序
        ...
    ]
````

* dateGroupBy方法是aggs的date_histogram类型聚合的封装  
````php
    public function dateGroupBy(string $field,string $interval = 'day',string $format = "yyyy-MM-dd",array $appendParams = [], ... $subGroups) :self
    
    ->dateGroupBy('news_posttime','day','yyyy-MM-dd')
````

* cardinality方法是aggs的cardinality类型聚合的封装
````php
    public function cardinality(string $field,array $appendParams = []) :self
    
    ->cardinality('news_sim_hash')
````

* avg方法是aggs的avg类型聚合的封装
````php
    public function avg(string $field,array $appendParams = []) :self
    
    ->avg('media_CI')
````

* sum方法是aggs的sum类型聚合的封装
````php
    public function sum(string $field,array $appendParams = []) :self
    
    ->sum('media_CI')
````

* min方法是aggs的min类型聚合的封装
````php
    public function min(string $field,array $appendParams = []) :self
    
    ->min('media_CI')
````

* max方法是aggs的max类型聚合的封装
````php
    public function max(string $field,array $appendParams = []) :self
    
    ->max('media_CI')
````

* stats方法是aggs的stats类型聚合的封装
````php
    public function stats(string $field,array $appendParams = []) :self
    
    ->stats('media_CI')
````

* extendedStats方法是aggs的extended_stats类型聚合的封装
````php
    public function extendedStats(string $field,array $appendParams = []) :self
    
    ->extendedStats('media_CI')
````

* topHits方法是top_hits类型聚合的封装
```php
    public function topHits($params) :self
    
    ->topHits([
        'from' => 2,
        'size' => 1,
        'sort' => ['news_posttime' => ['order' => 'asc']],
        '_source' => ['news_title','news_posttime','news_url','news_digest'],
        'highlight' => [
            'require_field_match'=>true,
            'pre_tags'=>'<h3>',
            'post_tags'=>'</h3>',
            'fields' => [
                'news_title' => new \stdClass(),
                'news_digest' => ['number_of_fragments' => 0]]
            ]
    ]);
    
    ->topHits(function(Es $query){
        $query->size(1)->from(2)
            ->orderBy('news_posttime','asc')
            ->select(['news_title','news_posttime','news_url','news_digest'])
            ->highlight('news_title')
            ->highlight('news_digest',['number_of_fragments' => 0]);
    })
```

* aggsFilter方法是filter类型聚合的封装，可在聚合内部进行条件过滤，$wheres参数仅支持数组和闭包，可参考where方法
```php
    public function aggsFilter($alias,$wheres,... $subGroups) :self
    
    ->aggsFilter('alias1',function(Es $query){
        $query->where('platform','web');
    },function(Es $query){
        $query->groupBy('platform_name',['size'=>30]);
    })
    ->aggsFilter('alias2',['platform'=>'web','news_title'=>'合肥',['news_postdate','>=','2020-09-01']],function(Es $query){
        $query->groupBy('platform_name',['size'=>30]);
    })
```

#### raw
* 原生dsl语句查询,不支持添加其他条件
```php
    public function raw($dsl) :self
    
    ->raw(['query'=>['match_all' => new \stdClass()]])->get()
    ->raw(json_encode(['query'=>['match_all' => new \stdClass()]]))->get()
```

#### dsl
* 返回待查询的dsl语句，$type = 'json'，返回json字符串
```php
    public function dsl($type = 'array')
```

#### get
* 查询结果，$directReturn = true，返回未经处理的结果
```php
    public function get($directReturn = false)
    
    // $directReturn = false时，返回以下数据
    [
        'total'     => 文档总数,
        'list'      => 文档列表,
        'aggs'      => 聚合结果（存在聚合时返回）,
        'scroll_id' => scroll_id（游标查询时返回）
    ]
```

#### paginator 分页
* paginator方法和collapse方法一起使用时，paginator方法内部会对折叠的字段做cardinality聚合，不需要考虑collapse的总数问题
```php
    public function paginator(int $page = 1, int $size = 10)
    
    ->collapse('news_sim_hash')->paginator()
    
    [
        'total'             => 文档总数（存在collapse时，是计算折叠后的总数）,
        'original_total'    => 文档总数（不受collapse影响）,
        'per_page'          => 每页条数,
        'current_page'      => 当前页码,
        'last_page'         => 最大页码,
        'list'              => 文档列表,
        'aggs'              => 聚合结果（存在聚合时返回）
    ]
```

#### first
* 返回第一条记录，$directReturn = true，返回未经处理的结果
```php
    public function first($directReturn = false)
```

#### count 计数 
```php
    public function count()
```

#### scroll 游标
```php
    $data = Es::init()->scroll()->size(1000)->where('platform','app')->get();
    $es = Es::init();
    while(true){
        $data = $es->scrollId($data['scroll_id'])->get();
        // do something
        ...
    }
```

## 封装示例
```php
    // 本例实现的是多个关键词组短语匹配，词组之间是or关系，词组内为and关系
    $keywordGroups = [
        ['中国','上海'],
        ['安徽','合肥'],
    ];
    public function keywords($keywordGroups,$type = 'full'){
        $this->where(function(self $query)use($keywordGroups,$type){
            foreach($keywordGroups as $keywordGroup){
                $query->orWhere(function(self $query1)use($keywordGroup,$type){
                    foreach($keywordGroup as $keyword){
                        if('full' == $type){
                            $query1->whereMultiMatch(['news_title','news_content'],$keyword,'phrase',["operator" => "OR"]);
                        }elseif('title' == $type){
                            $query1->whereMatch('news_title',$keyword,'match_phrase');
                        }elseif('content' == $type){
                            $query1->whereMatch('news_content',$keyword,'match_phrase');
                        }
                    }
                });
            }
        });
        return $this;
    }

    // 本例实现的是排除关键词组内的关键词
    $keywords = ['美国','日本'];
    public function keywordsExclude($keywords){
        $this->where(function(self $query)use($keywords){
            foreach($keywords as $keyword){
                $query->whereNotMultiMatch(['news_title','news_content'],$keyword,'phrase',["operator" => "OR"]);
            }
        });
        return $this;
    }
```
## query、scrollQuery实现示例
```php
    public function query()
    {
        if(!is_string($this->dsl)){
            $this->dsl = json_encode($this->dsl,JSON_UNESCAPED_UNICODE);
        }
        
        /****用内部组装好的$this->dsl进行查询，并返回es的响应...****/

        return $response;
    }
```

## 调用示例
```php
    Es::init()->select('id','name')->where('id',3)->dsl();
    Es::init()->select('id','name')->where('id',3)->groupBy('platform_name')->get();
    Es::init()->select('id','name')->where('id',3)->paginator(2,15);
    Es::init()->select('id','name')->where('id',3)->first();
    Es::init()->select('id','name')->where('id',3)->count();
    
    Es::init()->select('news_title','news_url','news_uuid','platform')
    ->where('platform',['wx','web','app'])
    ->whereBetween('news_postdate',['2020-09-01','2020-09-10'])
    ->keywords([['中国','上海'],['安徽','合肥']],'full')
    ->keywordsExclude(['美国','日本'])
    ->highlight('news_title')
    ->groupBy('platform',['size'=>20,'order'=>['_count'=>'asc']],function(Es $query){
        $query->groupBy('platform_name',['size'=>30]);
    },function(Es $query){
        $query->groupBy('platform_domian_pri',['size'=>30],function(Es $query){
            $query->topHits(['size'=>1]);
        });
    })
    ->dateGroupBy('news_posttime')
    ->aggs('news_like_count','histogram',['interval'=>100])
    ->cardinality('news_sim_hash')
    ->avg('media_CI')
    ->sum('media_CI')
    ->max('media_CI')
    ->min('media_CI')
    ->extendedStats('media_CI')
    ->get();
```
