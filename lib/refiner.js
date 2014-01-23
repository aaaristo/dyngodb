var _= require('underscore'),
    _deep= require('./deep'),
    diff = require('deep-diff').diff,
    async= require('async');

const  _compare= function (x, y)
       {
           if (x===y)
             return 0;

           return x > y ? 1 : -1; 
       },
       _collect= function (consume)
       {
          return function (cons) 
          { 
            _.keys(cons).forEach(function (table)
            {
                var c, tcons= cons[table];

                if (!(c=consume[table]))
                  c= consume[table]= { read: 0, write: 0 };

                c.read+= tcons.read;
                c.write+= tcons.write;
            }); 
          };
       },
       _ignoreNotFound= function (done,$id,$pos,query)
       {
            return function (err)
            {
                if (err.code=='notfound')
                {
                  query.identity.set($id,$pos,undefined);
                  done();
                }
                else
                  done(err);
            };
       },
       _limit= function (items,query)
       {
            if (query.limit&&(items.length+query.$returned>query.limit)&&!query.limited)
            {
                if (!query.count) console.log('client side limit'.red);
                items= items.slice(0,query.limit-query.$returned);
                query.limited= true;
            }

            if (query.skip&&!query.skipped)
            {
                console.log('client side skip'.red);
                items= items.slice(query.skip);
                query.skipped= true;
            }

            return items;
       },
       _oa = function(o, s) 
       {
             s = s.replace(/\[(\w+)\]/g, '.$1');
             s = s.replace(/^\./, '');
             var a = s.split('.');
             while (a.length) {
                 var n = a.shift();
                 if (n in o) {
                     o = o[n];
                 } else {
                     return;
                 }
             }
             return o;
       },
       _operator= function (cmp)
                  {
                      return function (fieldName,vals)
                             { 
                                  return function (item)
                                  {
                                      return cmp(_oa(item,fieldName),vals);
                                  };
                             };
                  },
       _operators= {
                      EQ: _operator(function (itemVal,vals)
                      {
                          return itemVal===vals[0];
                      }),

                      NE: _operator(function (itemVal,vals)
                      {
                          return itemVal!==vals[0];
                      }),

                      GT: _operator(function (itemVal,vals)
                      {
                          return itemVal>vals[0];
                      }),

                      GE: _operator(function (itemVal,vals)
                      {
                          return itemVal>=vals[0];
                      }),

                      LT: _operator(function (itemVal,vals)
                      {
                          return itemVal<vals[0];
                      }),

                      LE: _operator(function (itemVal,vals)
                      {
                          return itemVal<=vals[0];
                      }),

                      IN: _operator(function (itemVal,vals)
                      {
                         return _.contains(vals,itemVal);
                      }),

                      BEGINS_WITH: _operator(function (itemVal,vals)
                      {
                         return itemVal&&itemVal.indexOf(vals[0])==0;
                      }),

                      CONTAINS: _operator(function (itemVal,vals)
                      {
                         return itemVal&&itemVal.indexOf(vals[0])>-1;
                      }),

                      REGEXP: _operator(function (itemVal,vals)
                      {
                         return !!vals[0].exec(itemVal);
                      }),

                      BETWEEN: _operator(function (itemVal,vals)
                      {
                         return itemVal>=vals[0]&&itemVal<=vals[1];
                      })
                   },
       _filter= function (items,query)
       {
            var fieldNames= Object.keys(query.$filter);

            if (fieldNames.length)
            {

                console.log('filtering results in memory'.red);

                fieldNames.forEach(function (fieldName)
                {
                    var field= query.$filter[fieldName];
                    delete query.$filter[fieldName];
                    query.$filtered.push(fieldName);

                    items= _.filter(items,_operators[field.op](fieldName,field.values)); 
                });          

            }

            return items;
       },
       _sort= function (items,query)
       {
            if (query.orderby&&!query.sorted)
            {
               console.log('sorting results in memory'.red);

               var fields= query.$orderby;

               items.sort(function (x, y)
               {
                  var retval;

                  fields.some(function (field)
                  {
                      var fx= query.oa(x,field.name),
                          fy= query.oa(y,field.name);

                      if (fx!=fy)
                      {
                        retval= _compare(fx,fy)*field.dir;
                        return true;
                      }
                  });

                  return retval;
               }); 
            }

            return items;
       },
       _notfound= function (items)
       {
           var ret= _.filter(items,function (i) { return !!i.$id; }); 
           
           ret.next= items.next;

           return ret;
       },
       _modifiers= function (items,query)
       {
           items= _notfound(items);
           items= _filter(items,query);
           items= _sort(items,query);
           items= _limit(items,query);

           return items;
       },
       _refineItems= function (dyn,trigger,items,query)
       {
                if (query.canLimit())
                  items= _limit(items,query);
               
                var p= dyn.promise(null,null,'consumed'),
                    _load= function (key,proot,done)
                    {
                           var item= items[key];

                           query.identity.set(item.$id,0,item);

                           async.forEach(Object.keys(item),
                           function (field,done)
                           {
                               if (field.indexOf('$$$')==0)
                               {
                                 var attr= field.substring(3),
                                     $id= item[field];

                                 query.identity.get($id,'_',function (items)
                                 {
                                     if (items)
                                     {
                                         item[attr]= items;
                                         done();
                                     }
                                     else
                                         query.table.find({ $id: $id },
                                                          query.toprojection(proot[attr]),
                                                          query.identity)
                                         .results(function (values)
                                         {
                                            item[attr]= values;
                                            query.identity.set($id,'_',values);
                                            done();
                                         })
                                         .consumed(p.trigger.consumed)
                                         .error(_ignoreNotFound(done,$id,'_',query));
                                 });
                               }
                               else
                               if (field.indexOf('$$')==0)
                               {
                                 var attr= field.substring(2),
                                     parts= item[field].split('$:$'),
                                     $id= parts[0], $pos= 0;

                                 if (parts[1])
                                   $pos= +parts[1];

                                 query.identity.get($id,$pos,function (loaded)
                                 {
                                     if (loaded)
                                     {
                                         item[attr]= loaded; 
                                         done();
                                     }
                                     else
                                         query.table.findOne({ $id: $id },
                                                             query.toprojection(proot[attr]),
                                                             query.identity)
                                         .result(function (value)
                                         {
                                            item[attr]= value; 
                                            query.identity.set($id,$pos,value);
                                            done();
                                         })
                                         .consumed(p.trigger.consumed)
                                         .error(_ignoreNotFound(done,$id,$pos,query));
                                 });
                               }
                               else
                               if (field=='$ref')
                               {
                                  var $id, $pos= 0;

                                  if (query.noderef)
                                  {
                                     if (!item.$id)
                                       item= _.extend(item,item.$ref);

                                     delete item['$ref'];
                                     done();
                                     return;
                                  }

                                  if (typeof item.$ref=='string') 
                                  {
                                     var parts= item.$ref.split('$:$');
                                     $id= parts[0];

                                     if (parts[1])
                                       $pos= +parts[1];
                                  }
                                  else
                                  {
                                    $id= item.$ref.$id; 
                                    $pos= item.$ref.$pos; 
                                  }

                                  var _diff= diff(query.toprojection(proot),{ $id: 1, $pos: 1 });

                                  if (_diff)
                                      query.identity.get($id,$pos,function (loaded)
                                      {
                                         if (loaded)
                                         {
                                             item= loaded; 
                                             done();
                                         }
                                         else
                                             query.table.findOne({ $id: $id, $pos: $pos },
                                                                   query.toprojection(proot),
                                                                   query.identity)
                                             .result(function (loaded)
                                             {
                                                item= loaded;
                                                query.identity.set($id,$pos,loaded);
                                                done();
                                             })
                                             .consumed(p.trigger.consumed)
                                             .error(_ignoreNotFound(done,$id,$pos,query));
                                      });
                                  else
                                  {
                                    item= { $id: $id, $pos: $pos };
                                    done();
                                  }
                               }
                               else
                                 done();
                           },
                           function (err)
                           {
                              if (err) done(err); 
                              else
                              {
                                items[key]= item;
                                done();
                              }
                           });
                    },
                    _refine= function (key, done)
                    { 
                       _load(key, query.projection.root,
                       function (err)
                       {
                          var item= items[key];
                          query.identity.set(item.$id,0,item);

                          if (err)
                            done(err);
                          else
                            done();
                       });

                    }; 

                if ((query.projection.exclude || []).length>0)
                  _refine= _.wrap(_refine,function (wrapped,key,done)
                           {
                               var item= items[key];
                               items[key]= item.$ref ? item : _.omit(item,query.projection.exclude);

                               wrapped(key,
                               function (err)
                               {
                                  if (err) done(err);
                                  else
                                  {
                                    var item= items[key];
                                    items[key]= item.$ref ? _.omit(item,query.projection.exclude) : item;
                                    done();
                                  }
                               });
                           });

                 _refine= _.wrap(_refine,function (wrapped,key,done)
                          {
                               wrapped(key,
                               function (err)
                               {
                                  if (err) done(err)
                                  else
                                  {
                                    var item= items[key];
console.log(JSON.stringify(item,null,2));
                                    _deep.clone(item,function (clone)
                                    {
console.log(JSON.stringify(item,null,2));
console.log(JSON.stringify(clone,null,2));
                                       item.$old= clone;
                                       done();
                                    });
                                  }
                               });
                          });

                async.forEach(_.range(items.length),
                _refine,
                function (err)
                {
                   if (err)
                     refiner.trigger.error(err);
                   else
                     trigger(_modifiers(items,query));
                });

                return p;
       };

module.exports= function (dyn, query)
{
    var promises= ['results','end'];

    if (query.count) promises.push('count');

    var refiner= dyn.promise(promises,null,'consumed'), fended, ended, _end= refiner.trigger.end, _results, _items, consume= {}, _consumed= refiner.trigger.consumed;

    refiner.trigger.end= _.wrap(refiner.trigger.end,function (trigger)
    {
        fended= true;
    });

    refiner.trigger.consumed= _.wrap(refiner.trigger.consumed,function (trigger,cons)
    {
        var c;

        if (!(c=consume[cons.table]))
          c= consume[cons.table]= { read: 0, write: 0 };

        c.read+= cons.read;
        c.write+= cons.write;
    });

    refiner.trigger.results= _.wrap(refiner.trigger.results,function (trigger,items)
    {
        _results= trigger;

        if (!ended)
        process.nextTick(function () // so we can call _end
        {
            _refineItems(dyn,function (items)
            {
                 if (items.next&&!query.limited)
                   items.next();
                 
                 ended= query.limited || fended; 

                 delete items.next;

                 query.$returned+= items.length;

                 if (query.count)
                 {
                     process.stdout.write(('\r'+query.$returned).yellow);

                     if (query.canCount())
                     {
                       _consumed(consume);
                       refiner.trigger.count(query.$returned);
                     }
                 }
                 else
                 if (items.length>0)
                   _results(items);

                 if (ended) 
                 {
                    if (query.$returned==0)
                      _results([]);

                    _consumed(consume);
                    _end();
                 }
            },
            items,query).consumed(_collect(consume));
        });
    });
    
    return refiner;
};
