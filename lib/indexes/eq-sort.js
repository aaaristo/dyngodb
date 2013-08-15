var _= require('underscore'),
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
         _fields= _.collect(Object.keys(fields),
                            function (fieldName)
                            { 
                                var type= fields[fieldName];

                                if (type.secondary) 
                                  return { name: fieldName, 
                                           type: type.type,
                                      secondary: true };
                                else
                                  return { name: fieldName, 
                                           type: type };
                            }),
         primaryFieldNames= [],
         secondaryFieldNames= [];

     index.name= table._dynamo.TableName+'--'
                 +_.collect(_fields,
                            function (field) { return field.secondary ?
                                                      '__'+field.name : 
                                                      field.name; })
                   .join('-').replace(/\$/g,'_');

     var unhandled= false;

     _fields.every(function (field)
     {
         if (field.secondary)
           secondaryFieldNames.push(field.name);
         else
           primaryFieldNames.push(field.name);

         if (!_.contains(['S','N'],field.type))
           return !(unhandled=true);
        
         return true; 
     });

     if (unhandled) return false;

     index.create= function (done)
     {
            var _secondary= function ()
            {
                var secondaryFields= _.filter(_fields,function (field) { return !!field.secondary; });
                return _.collect(secondaryFields,
                       function (field)
                       {
                            return { name: field.name, 
                                     key: { name: field.name,
                                            type: field.type },
                                     projection: ['$id','$pos','$version'] };
                       });    
            };

            dyn.table(index.name)
               .hash('$hash','S')
               .range('$range','S')
               .create(function indexItems()
               {
                  dyn.table(index.name)
                     .hash('$hash','xx')
                     .query(function ()
                  {
                     done(new Error('Should not find anything!'));
                  })
                  .error(function (err)
                  {
                     if (err.code=='ResourceNotFoundException')
                       setTimeout(indexItems,5000);
                     else
                     if (err.code=='notfound')
                       table.find().results(function (items)
                       {
                           async.forEach(items,index.put,done);
                       })
                       .error(done);
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

     index.makeHash= function (item)
     {
        return _.collect(primaryFieldNames,
                         function (fieldName) { return _oa(item,fieldName); }).join(':');
     };

     index.makeCondHash= function (cond)
     {
        return _.collect(primaryFieldNames,
                         function (fieldName) { return cond[fieldName]; }).join(':');
     };

     index.makeRange= function (item)
     {
        return item.$id+':'+item.$pos;
     };

     index.indexable= function (item)
     {
        return item&&!_.some(primaryFieldNames,
                       function (fieldName) { return !_oa(item,fieldName); });
     };

     index.usable= function (cond)
     {
        return cond&&!_.some(primaryFieldNames,
                       function (fieldName) { return !cond[fieldName]||(cond[fieldName] instanceof RegExp); });
     };

     index.makeElement= function (item)
     {
        return _.extend(_.pick(item,_.union(['$id','$pos','$version'],
                                            secondaryFieldNames)),
                        { '$hash': index.makeHash(item),
                          '$range': index.makeRange(item) });
     };

     index.find= function (query)
     {
       var p= dyn.promise('results'),
           hash= index.makeCondHash(query.cond),
           tab= dyn.table(index.name)
                   .hash('$hash',hash),
           sort;


       if (query.orderby)
         Object.keys(query.orderby).every(function (field)
         {
             sort= { field: field, asc: query.orderby[field]>0 };
             return false;
         });

       if (sort)
         tab.index(sort.field);

       tab.query(p.trigger.results,
       { attrs: ['$id','$pos'], desc: sort&&!sort.asc, limit: query.limit })
       .error(p.trigger.error);

       return p;

     };

     return index; 
};

