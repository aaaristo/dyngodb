var _= require('underscore'),
    ret= require('ret'),
    async= require('async');

const  _oa = function(o, s) 
       {
             s = s.replace(/\[(\w+)\]/g, '.$1'); // convert indexes to properties
             s = s.replace(/^\./, '');           // strip a leading dot
             var a = s.split('.');
             while (a.length) {
                 var n = a.shift();
                 if (n in o) {
                     o = o[n];
                 } else {
                     return [];
                 }
             }

             if (!Array.isArray(o)) o= [o];

             return o;
       },
       _cast= function (v)
       {
         if (typeof v=='boolean')
           return v ? 1 : 0; 
         else
           return v; 
       },
       _cartesian= function() // cartesian product of N array
       {
            return _.reduce(arguments, function(a, b) {
                return _.flatten(_.map(a, function(x) {
                    return _.map(b, function(y) {
                        return x.concat([y]);
                    });
                }), true);
            }, [ [] ]);
       },
       combine = function(a, min) {
            var fn = function(n, src, got, all) {
                if (n == 0) {
                    if (got.length > 0) {
                        all[all.length] = got.sort();
                    }
                    return;
                }
                for (var j = 0; j < src.length; j++) {
                    fn(n - 1, src.slice(j + 1), got.concat([src[j]]), all);
                }
                return;
            }
            var all = [];
            for (var i = min; i < a.length; i++) {
                fn(i, a, [], all);
            }
            all.push(a.sort());
            return all;
       },
       NONWORDRE= /[^\w, ]+/,
       ngrams = function(value, gramSize)
       {
            gramSize = gramSize || 3;

            var simplified = '-' + value.toLowerCase().replace(NONWORDRE, '') + '-',
                lenDiff = gramSize - simplified.length,
                results = [];

            if (lenDiff > 0)
              for (var i = 0; i < lenDiff; ++i)
                 value += '-';
              
            for (var i = 0; i < simplified.length - gramSize + 1; ++i)
                results.push(simplified.slice(i, i + gramSize));
            
            return results;
       },
       join= function (dyn,queries)
       {
           var p= dyn.promise(['results','end'],null,'consumed'),
               leading= queries.shift(); // @TODO: get an hint
               sync= dyn.syncResults(function (err)
               {
                    if (err)
                      p.trigger.error(err);
                    else
                      p.trigger.end();
               });

           leading
           ({
                results: sync.results(function (results,done)
                {
                   if (!results.length)
                   {
                      var r= [];
                      r.next= function () { if (results.next) results.next(); done(); };
                      p.trigger.results(r);
                      return;
                   }

                   var ranges= _.pluck(results,'_range').sort(),
                       btw= [_.first(ranges),_.last(ranges)],
                       idx= {},
                       _vote= function (v)
                       {
                           var r= idx[v._range];
                           if (r) r._voted++;
                       }; 

                   results.forEach(function (r)
                   {
                      idx[r._range]= r;
                      r._voted= 0;
                   });

                   async.forEachSeries(queries,
                   function (qry,done)
                   {
                      var first, last;

                      if (btw)
                          qry
                          ({
                              results: function (res)
                              {
                                 res.forEach(_vote);
                                 if (!first) first= _.first(res);
                                 last= _.last(res);
                                 if (res.next) res.next(); // if leading query is ordered btw ids may be distant
                              },
                              error: done,
                              consumed: p.trigger.consumed,
                              end: function ()
                              {
                                 if (first)
                                 {
                                   btw[0]= first._range;
                                   btw[1]= last._range;
                                 }
                                 else
                                   btw= undefined;
                               
                                 done();
                              }
                          },btw);
                      else
                         done();
                   },
                   function (err)
                   {
                      if (err)
                        done(err);
                      else
                      {
                         var voted= _.where(results,{ _voted: queries.length });
                             joined= _.collect(voted,function (item)
                                     { 
                                       return _.pick(item,['_id','_pos']);
                                     });

                         joined.next= function () { if (results.next) results.next(); done(); };

                         p.trigger.results(joined);
                      }
                   });
                }),
                error: p.trigger.error,
                consumed: p.trigger.consumed,
                end: sync.end
           }); 

           return p;
       };


module.exports= function (dyn,table,fields)
{
     var $text= fields.$text, $combine= fields.$combine;

     fields= _.omit(fields,['$text','$combine']);

     var index= {},
         fieldNames= Object.keys(fields),
         lookupNames= [],
         _fields= _.collect(fieldNames,
                            function (fieldName)
                            { 
                                var type= fields[fieldName], pos= 0;

                                if ((pos=fieldName.indexOf('.'))>-1)
                                  lookupNames.push(fieldName.substring(0,pos));

                                return { name: fieldName, 
                                         type: type };
                            });

     index.name= 'fat-'+table._dynamo.TableName+'--'+fieldNames.join('-').replace(/\$/g,'_')+( $text ? '-text' : '');

     var unhandled= false;

     _fields.every(function (field)
     {
         if (!_.contains(['S','N','SS'],field.type))
           return !(unhandled=true);
        
         return true; 
     });

     if (unhandled) return false;

     index.create= function (done)
     {
            var _secondary= function ()
            {
                return _.collect(_.filter(_fields,function (field){ return field.type.length==1; }),  // not sets
                       function (field)
                       {
                            return { name: field.name.replace(/\$/g,'_'), 
                                     key: { name: field.name,
                                            type: field.type },
                                     projection: ['_id','_pos','_rev'] };
                       });    
            };

            if (index.dbopts.hints) console.log('This may take a while...'.yellow);

            dyn.table(index.name)
               .hash('_hash','S')
               .range('_range','S')
               .create(function indexItems()
               {
                  dyn.table(index.name)
                     .hash('_hash','xx')
                     .query(function ()
                  {
                       index.rebuild().error(done).success(done);
                  })
                  .error(function (err)
                  {
                     if (err.code=='ResourceNotFoundException')
                       setTimeout(indexItems,5000);
                     else
                       done(err);
                  });
               },{ secondary: _secondary() })
               .error(function (err)
               {
                   if (err.code=='ResourceInUseException')
                     setTimeout(function () { index.ensure(done) },5000);
                   else
                     done(err);
               });
     };

     index.makeRange= function (item)
     {
        return item._id+':'+item._pos;
     };

     index.makeKeys= function (item)
     {
        var keys= [], keyFieldNames= [], keyFieldValues= {};

        keys.push
        ({
           '_hash': 'VALUES',
           '_range': index.makeRange(item) 
        });

        fieldNames.some(function (fieldName)
        {
            keyFieldNames.push(fieldName);

            var val= keyFieldValues[fieldName]= _oa(item,fieldName),
                _combine= $combine ? function (val) { return $combine(item,val,combine); } : combine;

            if (val.length)
              _cartesian.apply(null,_.collect(_.values(keyFieldValues),function (val) { return _combine(val.sort(),1); })).forEach(function (hv)
              {
                  keys.push
                  ({
                       '_hash': _.collect(hv,
                                          function (vls)
                                          {
                                            return JSON.stringify(_.collect(vls,_cast));
                                          })
                                          .join(':'),
                       '_range': index.makeRange(item) 
                  });
              });
            else
              return true;
        });

        if ($text)
        {
           var textItem= $text(item); 

           _.keys(textItem).forEach(function (key)
           {
               var val= textItem[key],
                   words= (val+'').split(/\s+/);

               words.forEach(function (w)
               {
                   var tgs= _.uniq(ngrams(w,3));

                   tgs.forEach(function (tg)
                   {
                        keys.push
                        ({
                           '_hash': '$text$'+tg,
                           '_range': index.makeRange(item) 
                        });
                   });
               });
           }); 
           
        }

        return keys;
     };

     index.makeFilterHash= function (query)
     {
         var filter= query.$filter,
             values= [];

         fieldNames.some(function (fieldName)
         {
             var field= filter[fieldName];

             if (field&&_.contains(['ALL','EQ'],field.op))
             {
               values.push(JSON.stringify(_.collect(field.values,_cast).sort()));
               delete filter[fieldName];
               query.$filtered.push(fieldName); 
             }
             else
               return true;
         });

         return values.length ? values.join(':') : 'VALUES';
     };

     index.indexable= function (item)
     {
        return item&&($text||!!_oa(item,fieldNames[0]).length);
     };

     index.usable= function (query)
     {
        return query.cond&&(query.$text||!!query.cond[fieldNames[0]]);
     };

     index.makeElements= function (item)
     {
        return _.collect(index.makeKeys(item),
                         function (key)
                         {
                             return _.extend(_.pick(item,_.union(['_id','_pos','_rev'],fieldNames)),key);
                         });
     };

     index.put= function (item,done)
     {
         if (index.indexable(item))
         {
           var elems= index.makeElements(item);

           async.forEach(elems,function (elem, done)
           {
               dyn.table(index.name)
                  .hash('_hash',elem._hash)
                  .range('_range',elem._range)
                  .put(elem,done)
                  .error(done);
           },done);
         }
         else
           done(); 
     };

     index.streamElements= function ()
     {
        var _transform= function (items)
            {
               return _.flatten(_.collect(items,index.makeElements));
            },
            transform= _transform;

        if (lookupNames.length)
        {
          transform= function (items, cb)
          {
              async.forEach(_.range(items.length),
              function (idx,done)
              {
                  var item= items[idx];

                  async.forEach(lookupNames,function (name,done)
                  {
                     var attr= item['__'+name];

                     if (attr)
                     {
                       var parts= attr.split('$:$');
                       table.findOne({ _id: parts[0], _pos: (+parts[1]||0) })
                            .result(function (value)
                            {
                                item[name]= value; 
                                done();
                            })
                            .error(done); 
                     }
                     else
                       done();
                  },
                  done); 
              },
              function (err)
              {
                  if (err)
                    cb(err);
                  else
                    cb(null,_transform(items));
              });       
          };

          transform.async= true;
        }

        return index.tstream(transform);
     };

     index.update= function (item,op)
     {
          var iops= {},
              ops= iops[index.name]= [];

          if (index.indexable(item))
          {
              var elems= index.makeElements(item);

              if (index.indexable(item._old))
              {
                 var oldKeys= index.makeKeys(item._old);

                 oldKeys.forEach(function (key)
                 {
                     if (!_.findWhere(elems,key))
                       ops.push({ op: 'del', item: key });
                 });
              }

              elems.forEach(function (elem)
              {
                 ops.push({ op: op, item: elem });
              });
          }

          if (ops.length)
            return iops;
          else
            return undefined;
     };

     index.remove= function (item)
     {
         var p= dyn.promise(null,null,'consumed');

         if (index.indexable(item))
         {
           var keys= index.makeKeys(item);

           async.forEach(keys,
           function (key,done)
           {
               dyn.table(index.name)
                  .hash('_hash',key._hash)
                  .range('_range',key._range)
                  .delete(done)
                  .consumed(p.trigger.consumed)
                  .error(function (err)
               {
                  if (err.code=='notfound')
                    done();
                  else
                    done(err);
               });
           },
           p.should('success'));
         }
         else
           process.nextTick(p.trigger.success);

         return p;
     };

     index.find= function (query)
     {
       var p= dyn.promise(['results','count','end'],null,'consumed'),
           hash= index.makeFilterHash(query),
           tab= dyn.table(index.name)
                   .hash('_hash',hash),
           _index,
           sort,
           secondaryFieldName= _.first(_.intersection(_.difference(fieldNames,query.$filtered),Object.keys(query.$filter)));

       if (secondaryFieldName!==undefined)
       {
            var field= query.$filter[secondaryFieldName];

            if (field&&!_.contains(['IN','CONTAINS'],field.op))
            {
                delete query.$filter[secondaryFieldName];
                tab.range(secondaryFieldName,field.values,field.op)
                   .index(_index=secondaryFieldName.replace(/\$/g,'_'));
            }
       }
       
       if (query.$orderby)
       {
           var field= query.$orderby[0];

           if (_index)
           {
               if (_index==field.name)
                 sort= field;
           }
           else   
           if (_.contains(fieldNames,field.name))
           {
               tab.index(field.name.replace(/\$/g,'_'));
               sort= field;
           } 
       
           query.sorted= (!!sort)&&query.$orderby.length==1;
       }

       if (query.$text)
       {
         var words= query.$text.split(/\s+/), tgs= [];

         words.forEach(function (w)
         {
               tgs.push(_.uniq(ngrams(w,3)));
         }); 

         words= _.collect(words,function (w) { return w.toLowerCase().replace(NONWORDRE, ''); });

         tgs= _.filter(_.uniq(_.union.apply(_,tgs)),function (tg) { return tg[2]!='-'; });

         var queries= [];

          if (query.$filtered.length)
            queries.push(function (qry)
            {
                     tab.query(qry.results,
                     { attrs: ['_range','_id','_pos'],
                        desc: sort&&(sort.dir==-1),
                  consistent: query.$consistent,
                       limit: query.window,
                       count: undefined })
                     .error(qry.error)
                     .consumed(qry.consumed)
                     .end(qry.end);
            });

          tgs.forEach(function (tg)
          {
              var _forTg= function (fn) { fn.tg=tg; return fn; };

              queries.push(_forTg(function (qry,btw)
              {
                 var t= dyn.table(index.name)
                           .hash('_hash', '$text$'+tg);

                 if (btw)
                   t.range('_range',btw,'BETWEEN');

                 t.query(qry.results,
                 { attrs: ['_range','_id','_pos'],
              consistent: query.$consistent,
                   limit: query.window })
                 .error(qry.error)
                 .consumed(qry.consumed)
                 .end(qry.end);
              })); 
          });

          join(dyn,queries)
             .results(function (items)
             {
                items.refine= function (items)
                {
                    var refined= _.filter(items,
                                   function (item)
                                   {
                                       var textItem= $text(item),
                                           missing= words.slice();

                                       _.keys(textItem).every(function (key)
                                       {
                                           var val= textItem[key],
                                               vwords= _.collect((val+'').split(/\s+/),
                                                       function (w) { return w.toLowerCase()
                                                                              .replace(NONWORDRE, ''); });

                                           missing= _.filter(missing,function (w)
                                                    {
                                                           return !vwords.some(function (vw)
                                                           {
                                                              return vw.indexOf(w)==0;   
                                                           });
                                                    });

                                           return !!missing.length;
                                       }); 

                                       return !missing.length;
                                   }); 

                    refined.next= items.next;

                    return refined;
                };

                p.trigger.results(items);
             })
             .error(p.trigger.error)
             .consumed(p.trigger.consumed)
             .end(p.trigger.end);
       }
       else
       {
             query.counted= query.canCount();

             tab.query(query.count&&query.canCount() ? p.trigger.count : p.trigger.results,
             { attrs: ['_id','_pos'],
                desc: sort&&(sort.dir==-1),
          consistent: query.$consistent,
               limit: query.counted ? undefined : query.window,
               count: query.counted ? query.count : undefined })
             .error(p.trigger.error)
             .consumed(p.trigger.consumed)
             .end(p.trigger.end);
       }

       return p;

     };

     return index; 
};

