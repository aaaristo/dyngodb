var _= require('underscore'),
    async= require('async');

var __steps= function (rcurrent, rtarget, wcurrent, wtarget)
{
    var _steps= function (current, target)
        {
            var diff= target-current, 
                steps= diff>0 ? (diff > current ? [] : [target]) : [target];

            if (!steps.length)
            {
               while ((current=current*2)<target)
                   steps.push(current);

               var delta= target-steps[steps.length-1];

               if (delta>0)
                 steps.push(target);
            }

            return steps; 
        },
        _fill= function (arr,len)
        {
           var diff= len-arr.length,
               last= arr[arr.length-1];

           _(diff).times(function () { arr.push(last); });
        };

    var rsteps= _steps(rcurrent,rtarget),
        wsteps= _steps(wcurrent,wtarget);

    if (rsteps.length>wsteps.length)
      _fill(wsteps,rsteps.length);
    else
    if (wsteps.length>rsteps.length)
      _fill(rsteps,wsteps.length);

    return _.zip(rsteps,wsteps);
}

module.exports= function (dyn,table,read,write)
{
    var p= dyn.promise(),
        _check= function (cb)
        {
              dyn.describeTable(table,
              function (err,data)
              {
                  if (err)
                    p.trigger.error(err);
                  else
                  if (data.Table.TableStatus=='UPDATING')
                    setTimeout(_check,5000,cb);
                  else
                  {
                    table._dynamo= data.Table;
                    cb();
                  }
              });
        };

    if (!read||!write)
      process.nextTick(function ()
      { p.trigger.error(new Error('You should specify read and write ProvisionedThroughput')) });
    else
        dyn.describeTable(table,
        function (err,data)
        {
              if (err)
                  p.trigger.error(err);
              else
              {
                  var current= data.Table.ProvisionedThroughput;

                  if (current.ReadCapacityUnits==read
                    &&current.WriteCapacityUnits==write) 
                    p.trigger.success();
                  else
                  {
                     console.log('This may take a while...'.yellow);

                     var steps= __steps(current.ReadCapacityUnits,
                                        read,
                                        current.WriteCapacityUnits,
                                        write);

                     async.forEachSeries(steps,function (step,done)
                     {
                         var sread= step[0], swrite= step[1];

                         dyn.updateTable(table,sread,swrite,
                         function (err,data)
                         {
                            if (err)
                              done(err);
                            else
                              setTimeout(_check,5000,function ()
                              {
                                 if (sread==read&&swrite==write)
                                   console.log(('current capacity: '+sread+' read '+swrite+' write')
                                   .green);
                                 else
                                   console.log(('current capacity: '+sread+' read '+swrite+' write')
                                   .yellow);

                                 done();
                              });
                         });
                     },
                     p.should('success'));
                  }
              }
        });

    return p;
};
