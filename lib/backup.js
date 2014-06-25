var _= require('underscore'),
    carrier= require('carrier'),
    async= require('async');

module.exports= function (dyn,dbopts)
{
    var backup= {};

    backup.backup= function (table)
    {
        return function (opts)
        {
            var p= dyn.promise(),
                s3= require('./s3')(_.extend(opts,dbopts));

            s3.write(table+'/'+(new Date().toISOString())+'.dbk',        
            function (wstream)
            {
                var rstream= dyn.stream(table).scan({ limit: opts.limit });

                rstream
                   .on('data',function (items)
                   {
                       rstream.pause();
  
                       async.forEach(items,
                       function (item,done)
                       { 
                          if (!wstream.write(new Buffer(JSON.stringify(item)+'\n','utf8')))
                            wstream.on('drain',_.once(done));
                          else            
                            done();
                       },
                       function (err)
                       {
                          rstream.resume();
                       });
                   })
                   .on('end',function ()
                   {
                        wstream.end();
                   });

                wstream.on('close',p.trigger.success);
            });

            return p;
        };
    };

    backup.restore= function (table)
    {
        return function (opts)
        {
            var p= dyn.promise(),
                s3= require('./s3')(_.extend(opts,dbopts)),
                wstream= dyn.stream(table).mput('put');

            s3.read(opts.file,function (rstream)
            {
                carrier.carry(rstream, function (line)
                {
                    wstream.write([JSON.parse(line)]);
                },'utf8');

                rstream.on('end',p.trigger.success);
            }); 

            return p;
        };
    };

    return backup;     
};
