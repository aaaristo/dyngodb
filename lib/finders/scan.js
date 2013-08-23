var colors= require('colors'),
    _= require('underscore'),
    async= require('async');

const M= 1048576;

module.exports= function (dyn)
{
    var finder= {};

    finder.canFind= function (query)
    {
       return query.$supported;
    };

    finder.find= function (query)
    {
       console.log(('SCAN on '+query.table.name+' for '+JSON.stringify(query.cond,null,2)).red);

       var p= dyn.promise(['results','count']),
           avgItemSize= Math.ceil(query.table._dynamo.TableSizeBytes/query.table._dynamo.ItemCount),
           perWorker= (M/avgItemSize)*80/100,
           workers= Math.ceil(query.table._dynamo.ItemCount/perWorker);

       if (workers<1) workers= 1;

       // FIXME: implement opts.maxworkers ?

       var filter= {};

       Object.keys(query.$filter).forEach(function (fieldName)
       {
           var field= query.$filter[fieldName];

           if (field.op!='REGEXP')
           {
               filter[fieldName]= field;
               delete query.$filter[fieldName];
               query.$filtered.push(fieldName);
           }
       });
       
       if (query.count)
       {
           var count= 0;

           async.forEach(_.range(workers),
           function (segment,done)
           {
               dyn.table(query.table.name)
                  .scan(function (wcount)
                  {
                      count+=wcount; 
                      done();
                  },
                  { 
                    filter: query.$filter,
                     attrs: query.projection.include,
                     count: query.count,
                   segment: { no: segment, of: workers }
                  })
                  .error(done);
           },
           function (err)
           {
              if (err)
                p.trigger.error(err);
              else
                p.trigger.count(count);
           });
       }
       else
       {
           var results= [];

           async.forEach(_.range(workers),
           function (segment,done)
           {
               dyn.table(query.table.name)
                  .scan(function (wresults)
                  {
                      results[segment]= wresults;
                      done();
                  },
                  { 
                    filter: filter,
                     attrs: query.projection.include,
                     count: query.count,
                   segment: { no: segment, of: workers }
                  })
                  .error(done);
           },
           function (err)
           {
              if (err)
                p.trigger.error(err);
              else
                p.trigger.results(_.flatten(results));
           });
       }

       return p;
    };

    return finder;
};
