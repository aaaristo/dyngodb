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
     var index= {}, field= {}, fieldNames= _.keys(fields);

     field.name= fieldNames[0];
     field.type= fields[fieldNames[0]];

     if (!(field.type=='BW' && fieldNames.length==1)) return false;

     var _secondary= function ()
     {
        return [{
                 name: field.name, 
                 key: { name: field.name,
                        type: 'S' },
                 projection: ['$id','$pos','$version']
               }];
     };
   
     index.name= table._dynamo.TableName+'--BW-'+field.name;

     index.create= function (done)
     {
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
        return 'VALUES';
     };

     index.makeCondHash= function (cond)
     {
        return 'VALUES';
     };

     index.makeRange= function (item)
     {
        return item.$id+':'+item.$pos;
     };

     index.indexable= function (item)
     {
        return !!_oa(item,field.name);
     };

     index.usable= function (cond)
     {
        var _isBW= function (rex)
            {
                  var tks= ret(rex.source),
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
                    cond[field.name]= _chars(1);
                    return true;
                  }
                  else 
                    return false;
            };

        return !!cond[field.name]
               &&(cond[field.name] instanceof RegExp)
               &&_isBW(cond[field.name]);
     };

     index.makeElement= function (item)
     {
        return _.extend(_.pick(item,['$id','$pos','$version',field.name]),
                        { '$hash': index.makeHash(item),
                          '$range': index.makeRange(item) });
     };

     index.find= function (query)
     {
       var p= dyn.promise(['results','count']),
           hash= index.makeCondHash(query.cond);
           tab= dyn.table(index.name)
                   .hash('$hash',hash);

       if (query.cond[field.name]!='')
          tab.range(field.name,query.cond[field.name],'BEGINS_WITH')

       tab.index(field.name)
          .query(query.count ? p.trigger.count : p.trigger.results,
                { attrs: ['$id','$pos'], limit: query.limit, count: query.count })
          .error(p.trigger.error);

       return p;
     };

     return index; 
};

