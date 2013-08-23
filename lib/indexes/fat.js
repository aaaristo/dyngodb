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

     index.name= table._dynamo.TableName+'--'fieldNames.join('-').replace(/\$/g,'_');

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
                return _.collect(_fields.slice(1),
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
        var keys= [];

        item. 
                { '$hash': '$'+_.collect(keyFieldNames,
                           function (fieldName) { return _oa(item,fieldName); })
                           .join(':'),
                  '$range': index.makeRange(item) }

        return keys;
     };

     index.makeCondHash= function (cond)
     {
        return _.collect(primaryFieldNames,
                         function (fieldName) { return cond[fieldName]; }).join(':');
     };

     index.indexable= function (item)
     {
        return item&&!_oa(item,fieldNames[0]);
     };

     index.usable= function (cond)
     {
        return cond&&!_.some(primaryFieldNames,
                       function (fieldName) { return !cond[fieldName]||(cond[fieldName] instanceof RegExp); })
                   &&!_.some(Object.keys(cond),function (fieldName) { return !_.findWhere(_fields,{ name: fieldName }); });
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
              var elem= index.makeElement(item);

              if (index.indexable(item.$old))
              {
                 var old= index.makeElement(item.$old);

                 if (elem.$hash!=old.$hash
                   ||elem.$range!=old.$range)
                  ops.push({ op: 'del', item: _.pick(old,['$hash','$range']) });
              }

              ops.push({ op: 'put', item: elem });
          }

          if (ops.length)
            return iops;
          else
            return undefined;
     };

     index.remove= function (item,done)
     {
         if (index.indexable(item))
           dyn.table(index.name)
              .hash('$hash',index.makeHash(item))
              .range('$range',index.makeRange(item))
              .delete(done)
              .error(function (err)
           {
              if (err.code=='notfound')
                done();
              else
                done(err);
           });
         else
           done();
     };

     index.find= function (query) // @FIXME: should be rewritten
     {
       var p= dyn.promise(['results','count']),
           hash= index.makeCondHash(query.cond),
           tab= dyn.table(index.name)
                   .hash('$hash',hash),
           _index,
           canLimit= true,
           sort;

       if (secondaryFieldNames[0]&&query.cond[secondaryFieldNames[0]]!==undefined)
       {
            var field= secondaryFieldNames[0],
                val= query.cond[field],
                type= typeof val,
                op= 'EQ';

            tab.index(field);
            _index= field;

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
       
       if (query.$orderby)
       {
           var field= query.$orderby[0];

           if (_index)
           {
               if (_index==field.name)
                 sort= field;
           }
           else   
           if (_.contains(secondaryFieldNames,field.name))
           {
               tab.index(field.name);
               sort= field;
           } 
       
           query.sorted= (!!sort)&&query.$orderby.length==1;

           if (!query.sorted)
             canLimit= false;
       }

       tab.query(query.count ? p.trigger.count : p.trigger.results,
       { attrs: ['$id','$pos'], desc: sort&&(sort.dir==-1), limit: canLimit ? query.limit : undefined, count: query.count })
       .error(p.trigger.error);

       return p;

     };

     return index; 
};

