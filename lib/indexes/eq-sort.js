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
                       function (fieldName) { return !cond[fieldName]||(cond[fieldName] instanceof RegExp); })
                   &&!_.some(Object.keys(cond),function (fieldName) { return !_.findWhere(_fields,{ name: fieldName }); });
     };

     index.makeElement= function (item)
     {
        return _.extend(_.pick(item,_.union(['$id','$pos','$version'],
                                            secondaryFieldNames)),
                        { '$hash': index.makeHash(item),
                          '$range': index.makeRange(item) });
     };

     index.find= function (query) // @FIXME: should be rewritten
     {
       var p= dyn.promise(['results','count']),
           hash= index.makeCondHash(query.cond),
           tab= dyn.table(index.name)
                   .hash('$hash',hash),
           sort;

       if (secondaryFieldNames[0]&&query.cond[secondaryFieldNames[0]]!==undefined)
       {
            var field= secondaryFieldNames[0],
                val= query.cond[field],
                type= typeof val,
                op= 'EQ';

            tab.index(field);

            if (type=='object')
            {
                if (val instanceof RegExp)
                {
                      var tks= ret(val.source),
                          _chars= function (start)
                          {
                              return _.collect(tks.stack.slice(start),
                                       function (tk) { return String.fromCharCode(tk.value); })
                                      .join(''); 
                          };

                      if (tks.stack
                          &&tks.stack[0]
                          &&tks.stack[0].type==ret.types.POSITION
                          &&tks.stack[0].value=='^'
                          &&!_.filter(tks.stack.slice(1),function (tk) { return tk.type!=ret.types.CHAR; }).length)
                      {
                        val= _chars(1);
                        op= 'BEGINS_WITH';
                      }
                      else
                        val='';
                } 
                else
                if (val.$ne!==undefined)
                { val= val.$ne; op='NE'; }
                else
                if (val.$gt!==undefined)
                { val= val.$gt; op='GT'; }
                else
                if (val.$lt!==undefined)
                { val= val.$lt; op='LT'; }
                else
                if (val.$gte!==undefined)
                { val= val.$gte; op='GE'; }
                else
                if (val.$lte!==undefined)
                { val= val.$lte; op='LE'; }
                else
                if (val.$in!==undefined)
                { val= val.$in; op='IN'; }
                else
                if (val.$exists!==undefined)
                { op= val.$exists ? 'NOT_NULL' : 'NULL'; val= undefined; }
                else
                {
                  p.trigger.error('unknown secondary operator on index');
                  return;
                }
            }

            if (val!=='')
              tab.range(field,val,op);
       }
       
       if (query.orderby)
         Object.keys(query.orderby).every(function (field)
         {
             sort= { field: field, asc: query.orderby[field]>0 };
             return false;
         });

       if (sort)
         tab.index(sort.field);

       tab.query(query.count ? p.trigger.count : p.trigger.results,
       { attrs: ['$id','$pos'], desc: sort&&!sort.asc, limit: query.limit, count: query.count })
       .error(p.trigger.error);

       return p;

     };

     return index; 
};

