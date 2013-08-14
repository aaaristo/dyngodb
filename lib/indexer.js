var _= require('underscore'),
    async= require('async');

var _indexes= [require('./indexes/eq-sort')];

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

     _fields.forEach(function (field)
     {
         if (field.secondary)
           secondaryFieldNames.push(field.name);
         else
           primaryFieldNames.push(field.name);
     });

     index.exists= function (done)
     {
          dyn.table(index.name)
             .hash('$hash','xx')
             .query(function ()
          {
             done(null,true);
          })
          .error(function (err)
          {
              if (err.code=='ResourceNotFoundException')
                done(null,false); 
              else
              if (err.code=='notfound')
                done(null,true); 
              else
                done(err);
          });
     };

     index.drop= function (done)
     {
         dyn.deleteTable(index.name,done);
     };

     index.rebuild= function (done)
     {
         index.drop(function (err)
         {
             if (err)
               done(err);
             else
               index.create(done);
         });
     };

     index.ensure= function (done)
     {
         index.exists(function (err, exists)
         { 
            if (err)
              done(err);
            else
            if (exists)
              done();
            else
              index.create(done);
         });
     };

     _indexes.every(function (canIndex)     
     {
        if (ind=canIndex(dyn,table,fields))
        {
          _.extend(index,ind);
          return false;
        }
        else
          return true;
     });

     if (index.create)
       return index; 
     else
       return false; 
};

