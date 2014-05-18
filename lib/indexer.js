var _= require('underscore'),
    async= require('async'),
    _modify= require('./capacity');

var _indexes= [require('./indexes/fat'),
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

module.exports= function (dyn,table,fields,dbopts)
{
     var index= { dbopts: dbopts };

     index.exists= function (done)
     {
          dyn.describeTable(index.name,function (err,data)
          {
              if (err)
              {
                  if (err.code=='ResourceNotFoundException')
                    done(null,false);
                  else
                    done(err);
              }
              else
              {
                index._dynamo= data.Table;
                done(null,true);
              }
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

         if (index.dbopts.hints) console.log('This may take a while...'.yellow);

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

     index.rebuild= function (window)
     {
           var p= dyn.promise(),
               sindex= dyn.stream(index.name),
               dcnt= 0,
               pcnt= 0,
               del= sindex.mput('del'),
               mput= sindex.mput('put'),
               limit= window || (index._dynamo ? index._dynamo.ProvisionedThroughput.WriteCapacityUnits : 25);

           sindex.scan({ attrs: ['_hash','_range'], limit: limit })
                 .on('data',function (items) { process.stdout.write(('\r'+(dcnt+=items.length)).red); })
                 .pipe(del)
                 .on('finish',function ()
           {
                console.log();

                dyn.stream(table._dynamo.TableName)
                   .scan({ limit: limit })
                   .pipe(index.streamElements())
                   .on('data',function (items) { process.stdout.write(('\r'+(pcnt+=items.length)).yellow); })
                   .on('error',p.trigger.error)
                   .pipe(mput)
                   .on('error',p.trigger.error)
                   .on('finish',_.compose(p.trigger.success,console.log));
           });

           return p;
     };

     index.empty= function (window)
     {
           var p= dyn.promise(),
               sindex= dyn.stream(index.name),
               dcnt= 0,
               del= sindex.mput('del'),
               limit= window || (index._dynamo ? index._dynamo.ProvisionedThroughput.WriteCapacityUnits : 25);

           sindex.scan({ attrs: ['_hash','_range'], limit: limit })
                 .on('data',function (items) { process.stdout.write(('\r'+(dcnt+=items.length)).red); })
                 .pipe(del)
                 .on('finish',_.compose(p.trigger.success,console.log));

           return p;
     };

     index.tstream= function (fn)
     {
           return dyn.stream(index.name).transform(fn);
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

     index.modify= function (read,write)
     {
        return _modify(dyn,index.name,read,write);
     };

     index.put= function (item,done)
     {
         if (index.indexable(item))
         {
           var elem= index.makeElement(item);

           dyn.table(index.name)
              .hash('_hash',elem._hash)
              .range('_range',elem._range)
              .put(elem,done)
              .error(done);
         }
         else
           done(); 
     };

     index.update= function (item,op)
     {
          var iops= {},
              ops= iops[index.name]= [];

          if (index.indexable(item))
          {
              var elem= index.makeElement(item);

              if (index.indexable(item._old))
              {
                 var old= index.makeElement(item._old);

                 if (elem._hash!=old._hash
                   ||elem._range!=old._range)
                  ops.push({ op: 'del', item: _.pick(old,['_hash','_range']) });
              }

              ops.push({ op: op, item: elem });
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
              .hash('_hash',index.makeHash(item))
              .range('_range',index.makeRange(item))
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

