var copy= require('knox-copy'),
    MultiPartUpload = require('knox-mpu'),
    stream = require('stream'),
    async = require('async'),
    _ = require('underscore'),
    fs= require('fs');

module.exports= function (opts)
{
    opts= _.defaults(opts || {},
    { 
         accessKeyId: process.env.AWS_ACCESS_KEY_ID, 
         secretAccessKey: process.env.AWS_SECRET_KEY || process.env.AWS_SECRET_ACCESS_KEY,
         region: process.env.AWS_REGION
    });

    var s3 = copy.createClient
        ({
            key: opts.accessKeyId
          , secret: opts.secretAccessKey
          , bucket: opts.bucket
          , region: opts.region
        }),
        file= {};

    file.write= function (path,cb)
    {
       var wstream = new stream.Stream()
       wstream.writable = true
       var rstream = new stream.Stream()
       rstream.readable = true;

       wstream.write = function (data)
       {
           rstream.emit('data',data);
           return true; // true means 'yes i am ready for more data now'
           // OR return false and emit('drain') when ready later
       };

       wstream.end = function (data)
       {
           if (data) rstream.emit('data',data);
           rstream.emit('end');
       };

       new MultiPartUpload
       ({
            client: s3,
            objectName: path,
            stream: rstream
       },
       function(err, res)
       {
          if (err)
            console.log('file.s3.write',err);

           wstream.emit('close');
       });

       cb(wstream);
    };

    file.read= function (path,cb)
    {
       s3.getFile(path, function(err,res)
       {
          cb(res);
       });
    };

    file.delete= function (path,cb)
    {
       s3.deleteFile(path, function(err, res)
       {
          if (err) throw err;
          cb();
       });
    };

    file.size= function (path, cb)
    {
       s3.headFile(path, function(err, res)
       {
          if (err) throw err;
          cb(parseInt(res.headers['content-length']));
       });
    };

    file.copyDir= function (src,dest,cb)
    {
       var queue= async.queue(function (key,done)
       {
            s3.copyFile(key,key.replace(src,dest),done);
       },10); 

       queue.drain= cb;

       var found= false;

       s3.streamKeys({ prefix: src })
         .on('data', function (key)
         { 
            found= true;
            queue.push(key,function (err) { console.log(err); });
         })
         .on('end', function ()
         {
            if (!found) cb();
         });
    };

    return file;
};
