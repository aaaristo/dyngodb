var _= require('underscore'),
    async= require('async');

var _indexes= [require('./indexes/eq-sort'),
               require('./indexes/begins-with'),
               require('./indexes/cloud-search')];

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
     var index= {};

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
         var _check= function (p)
         {
              dyn.describeTable(index.name,
              function (err,data)
              {
                  if (err)
                  {
                      if (err.code=='ResourceNotFoundException')
                        p ? p.trigger.success() : done();
                      else
                        p ? p.trigger.error(err) : done(err);
                  }
                  else
                    setTimeout(_check,5000,p);
              });
         };

         console.log('This may take a while...'.yellow);

         if (done)
           dyn.deleteTable(index.name,function (err)
           {
              if (err)
                done(err);
              else
                setTimeout(_check,5000);
           });
         else
         {
            var p= dyn.promise();

            dyn.deleteTable(index.name,function (err)
            {
                if (err)
                  p.trigger.error(err); 
                else
                  setTimeout(_check,5000,p);
            });

            return p;
         }
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

     index.put= function (item,done)
     {
         if (index.indexable(item))
         {
           var elem= index.makeElement(item);

           dyn.table(index.name)
              .hash('$hash',elem.$hash)
              .range('$range',elem.$range)
              .put(elem,done)
              .error(done);
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

     _indexes.every(function (canIndex)     
     {
        if (ind=canIndex(dyn,table,fields))
        {
          _.extend(ind,_.extend(index,ind));
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

