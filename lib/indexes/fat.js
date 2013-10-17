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
       };


module.exports= function (dyn,table,fields)
{
     var index= {},
         fieldNames= Object.keys(fields),
         _fields= _.collect(fieldNames,
                            function (fieldName)
                            { 
                                var type= fields[fieldName];

                                return { name: fieldName, 
                                         type: type };
                            });

     index.name= 'fat-'+table._dynamo.TableName+'--'+fieldNames.join('-').replace(/\$/g,'_');

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
                                     projection: ['$id','$pos','$version'] };
                       });    
            };

            console.log('This may take a while...'.yellow);

            dyn.table(index.name)
               .hash('$hash','S')
               .range('$range','S')
               .create(function indexItems()
               {
                  dyn.table(index.name)
                     .hash('$hash','xx')
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
        return item.$id+':'+item.$pos;
     };

     index.makeKeys= function (item)
     {
        var keys= [], keyFieldNames= [], keyFieldValues= {};

        keys.push
        ({
           '$hash': 'VALUES',
           '$range': index.makeRange(item) 
        });

        fieldNames.some(function (fieldName)
        {
            keyFieldNames.push(fieldName);

            var val= keyFieldValues[fieldName]= _oa(item,fieldName);

            if (val.length)
            {
              _cartesian.apply(null,_.collect(_.values(keyFieldValues),function (val) { return combine(val.sort(),1); })).forEach(function (hv)
              {
                  keys.push
                  ({
                       '$hash': _.collect(hv,
                                          JSON.stringify)
                                          .join(':'),
                       '$range': index.makeRange(item) 
                  });
              });
            }
            else
              return true;
        });

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
               values.push(JSON.stringify(field.values.sort()));
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
        return item&&!!_oa(item,fieldNames[0]).length;
     };

     index.usable= function (query)
     {
        return query.cond&&!!query.cond[fieldNames[0]];
     };

     index.makeElements= function (item)
     {
        return _.collect(index.makeKeys(item),
                         function (key)
                         {
                             return _.extend(_.pick(item,_.union(['$id','$pos','$version'],fieldNames)),key);
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
                  .hash('$hash',elem.$hash)
                  .range('$range',elem.$range)
                  .put(elem,done)
                  .error(done);
           },done);
         }
         else
           done(); 
     };

     index.streamElements= function ()
     {
        return index.tstream(function (items)
        {
           return _.flatten(_.collect(items,index.makeElements));
        });
     };

     index.update= function (item,op)
     {
          var iops= {},
              ops= iops[index.name]= [];

          if (index.indexable(item))
          {
              var elems= index.makeElements(item);

              if (index.indexable(item.$old))
              {
                 var oldKeys= index.makeKeys(item.$old);

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

     index.remove= function (item,done)
     {
         if (index.indexable(item))
         {
           var keys= index.makeKeys(item);

           async.forEach(keys,
           function (key,done)
           {
               dyn.table(index.name)
                  .hash('$hash',key.$hash)
                  .range('$range',key.$range)
                  .delete(done)
                  .error(function (err)
               {
                  if (err.code=='notfound')
                    done();
                  else
                    done(err);
               });
           },
           done);
         }
         else
           done();
     };

     index.find= function (query)
     {
       var p= dyn.promise(['results','count','end']),
           hash= index.makeFilterHash(query),
           tab= dyn.table(index.name)
                   .hash('$hash',hash),
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

       query.counted= query.canCount();

       tab.query(query.count&&query.canCount() ? p.trigger.count : p.trigger.results,
       { attrs: ['$id','$pos'],
          desc: sort&&(sort.dir==-1),
    consistent: query.$consistent,
         limit: query.counted ? undefined : query.window,
         count: query.counted ? query.count : undefined })
       .error(p.trigger.error)
       .end(p.trigger.end);

       return p;

     };

     return index; 
};

