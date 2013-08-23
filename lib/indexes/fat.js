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
                     return;
                 }
             }
             return o;
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
         if (!_.contains(['S','N'],field.type))
           return !(unhandled=true);
        
         return true; 
     });

     if (unhandled) return false;

     index.create= function (done)
     {
            var _secondary= function ()
            {
                return _.collect(_fields,
                       function (field)
                       {
                            return { name: field.name, 
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
                       table.find().results(function (items)
                       {
                           async.forEach(items,index.put,done);
                       })
                       .error(done);
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
        var keys= [], keyFieldNames= [];

        keys.push
        ({
           '$hash': 'VALUES',
           '$range': index.makeRange(item) 
        });

        fieldNames.some(function (fieldName)
        {
            keyFieldNames.push(fieldName);

            var val= _oa(item,fieldName);

            if (val!==''&&val!==undefined)
              keys.push
              ({
                   '$hash': _.collect(keyFieldNames,
                                      function (fieldName) { return JSON.stringify(_oa(item,fieldName)); })
                                      .join(':'),
                   '$range': index.makeRange(item) 
              });
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

             if (field&&field.op=='EQ')
             {
               values.push(JSON.stringify(field.values[0]));
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
        return item&&!!_oa(item,fieldNames[0]);
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

     index.update= function (item,done)
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
                 ops.push({ op: 'put', item: elem });
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
                  .hash('$hash',item.$hash)
                  .range('$range',item.$range)
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
       var p= dyn.promise(['results','count']),
           hash= index.makeFilterHash(query),
           tab= dyn.table(index.name)
                   .hash('$hash',hash),
           _index,
           canLimit= true,
           sort,
           secondaryFieldName= _.first(_.difference(fieldNames,query.$filtered));

       if (secondaryFieldName!==undefined)
       {
            var field= query.$filter[secondaryFieldName];

            if (field&&!_.contains(['IN','CONTAINS'],field.op))
            {
                delete query.$filter[secondaryFieldName];
                tab.range(secondaryFieldName,field.values,field.op)
                   .index(_index=secondaryFieldName);
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
               tab.index(field.name);
               sort= field;
           } 
       
           query.sorted= (!!sort)&&query.$orderby.length==1;

           if (!query.sorted)
             canLimit= false;
       }

       query.limited= query.canLimit();
       query.counted= query.canCount();

       tab.query(query.count&&query.canCount() ? p.trigger.count : p.trigger.results,
       { attrs: ['$id','$pos'],
          desc: sort&&(sort.dir==-1),
         limit: query.limited ? query.limit : undefined,
         count: query.counted ? query.count : undefined })
       .error(p.trigger.error);

       return p;

     };

     return index; 
};

