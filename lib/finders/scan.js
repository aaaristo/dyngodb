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
       if (query.opts.hints)
         console.log(('SCAN on '+query.table.name+' for '+JSON.stringify(query.cond,null,2)).red);

       var p= dyn.promise(['results','count','end'],null,'consumed'),
           avgItemSize= Math.ceil(query.table._dynamo.TableSizeBytes/query.table._dynamo.ItemCount),
           perWorker= (M/avgItemSize)*80/100,
           workers= Math.ceil(query.table._dynamo.ItemCount/perWorker);

       if (!workers) workers= 1;
       else
       if (workers>query.table._dynamo.ProvisionedThroughput.ReadCapacityUnits)
         workers= query.table._dynamo.ProvisionedThroughput.ReadCapacityUnits; 

       // FIXME: implement opts.maxworkers ? better divide & conquer algorithm 

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

       query.counted= query.canCount()&&query.count;
       
       if (query.counted)
       {

           var count= 0,
               progress= [],
               _progress= function (segment, pcount)
               {
                   progress[segment]= pcount;

                   process.stdout.write(('\r'+_.reduce(progress,function (memo,num) { return memo+num; },0)).yellow);
               };

           async.forEach(_.range(workers),
           function (segment,done)
           {
               var sp= dyn.table(query.table.name)
                          .scan(function (wcount)
                          {
                              count+=wcount; 
                              done();
                          },
                          { 
                            filter: filter,
                             attrs: query.finderProjection(),
                             limit: query.window,
                             count: query.count,
                           segment: { no: segment, of: workers }
                          })
                          .consumed(p.trigger.consumed)
                          .error(done);

               if (dyn.iscli())
                  sp.progress(function (pcount) { _progress(segment,pcount); })
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
           async.forEach(_.range(workers),
           function (segment,done)
           {
               dyn.table(query.table.name)
                  .scan(p.trigger.results,
                  { 
                    filter: filter,
                     attrs: query.finderProjection(),
                   segment: { no: segment, of: workers },
                     limit: query.window
                  })
                  .consumed(p.trigger.consumed)
                  .end(done)
                  .error(done);
           },
           function (err)
           {
              if (err)
                p.trigger.error(err);
              else
                p.trigger.end();
           });
       }

       return p;
    };

    return finder;
};
