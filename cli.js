#!/usr/local/bin/node

const NOPE= function (){};

var dyngo= require('./index'),
    async= require('async'),
    fs= require('fs'),
    lazy= require('lazy'),
    util= require('util'),
    readline= require('readline'),
    _= require('underscore'),
    path= require('path').join,
    colors = require('colors'),
    AWS = require('aws-sdk');

var argv = require('optimist').argv;

var _history= [];
      
const _json= function (path,content)
      {
          try
          {
              if (!content)
                return JSON.parse(fs.readFileSync(path,'utf8'));
              else
              {
                fs.writeFileSync(path,JSON.stringify(content,null,2),'utf8')
                return { success: function (fn) { process.nextTick(fn); } };
              }
          }
          catch (ex)
          {
              console.log((ex+'').red);
          }
      }, 
      _eval= function (cmd,db,last)
      {
        return eval('(function (db,last,_,json){ return '+cmd+'; })')(db,last,_,_json);
      },
      _dorc= function (db,cb)
      {
          var rcFile= path(getUserHome(),'.dyngorc'),
              localRcFile= '.dyngorc',
              last,
              _file= function (f, done)
              {
                 var _lines= [];

                 if (fs.existsSync(f))  
                 {
                    console.log(('executing '+f+'...').green);
                    var rstream= fs.createReadStream(f);

                    rstream.on('end',function ()
                    {
                        async.forEachSeries(_lines,
                        function (cmd,done)
                        {
                           console.log(cmd);

                           var promise= _eval(cmd,db,last);

                           if (!promise.success)
                             done(new Error('invalid rc command')); 
                           else
                             promise.success(function ()
                             {
                                 console.log('done!'.green);
                                 done();
                             });
                    },
                    done);
                    });

                    new lazy(rstream).lines.forEach(function (l) { _lines.push(l.toString('utf8')); });
                 }
                 else
                    done();
              };

          _file(rcFile,function (err)
          {
              if (err)
              {
                 console.log(err.message.red,err.stack);
                 process.exit(1);
              }
              else
              if (localRcFile!=rcFile)
                 _file(localRcFile,function (err)
                 {
                      if (err)
                      {
                         console.log(err.message.red,err.stack);
                         process.exit(1);
                      }
                      else
                         cb(); 
                 });
              else
                 cb();
          });
      },
      getUserHome= function() 
      {
          return process.env[(process.platform == 'win32') ? 'USERPROFILE' : 'HOME'];
      },
      getHistory= function()
      {
          var historyFile= path(getUserHome(),'.dyngodb_history');

          try
          {

              if (fs.existsSync(historyFile))
                _history.push
                .apply(_history,JSON.parse(fs.readFileSync(historyFile,'utf8')));

          }
          catch(ex)
          {}

          return _history;
      },
      saveHistory= function ()
      {
          var historyFile= path(getUserHome(),'.dyngodb_history');
        
          if (_history&&_history.length>0)
            fs.writeFileSync(historyFile,JSON.stringify(_history),'utf8');
      };

process.on('exit', saveHistory);
process.on('SIGINT', function () { saveHistory(); process.exit(0); });

var args= [function (err,db)
{
   var rl = readline.createInterface
   ({
      input: process.stdin,
      output: process.stdout,
      completer: function (linePartial, cb)
      {
          if (linePartial.indexOf('db.')==0)
          {
            var tables= _.collect(_.filter(_.keys(db),
                                  function (key) { return key.indexOf(linePartial.replace('db.',''))==0; }),
                        function (res) { return 'db.'+res; });
            cb(null,[tables, linePartial]); 
          }
          else
            cb(null,[[], linePartial]); 
      }
   });

   var last;

   if (err)
     console.log(err);
   else
   {
     rl.history= getHistory();

     _dorc(db,function ()
     {
         (function ask()
         {
             var _ask= function (fn)
                 {
                     return function ()
                     {
                        var args= arguments;
                        fn.apply(null,args); 
                        ask();
                     };
                 },
                 _print= function (obj,cb)
                 {
                     db.cleanup(obj).clean(function (obj)
                     {
                        console.log(util.inspect(obj,{ depth: null }));
                        cb();
                     });
                 };

             rl.question('> ', function (answer) 
             {

                if (!answer) { ask(); return; };
                
                if (answer.indexOf('show collections') > -1)
                { 
                   _.filter(_.keys(db),function (key) { return !!db[key].find; }).forEach(function (c) { console.log(c); });
                   ask();
                   return;
                }
                else
                if (answer=='clear')
                {
                   process.stdout.write('\u001B[2J\u001B[0;0f');
                   ask();
                   return;
                }

                try
                {
                   var time= process.hrtime(),
                       promise= _eval(answer,db,last),
                       end,
                       printed,
                       chunks= 0,
                       _doneres= function () { elapsed(); ask(); },
                       doneres= _.wrap(_doneres,function (done) {  if (printed&&end) done(); }),
                       elapsed= function ()
                       {
                          var diff= process.hrtime(time),
                              secs= (diff[0]*1e9+diff[1])/1e9;

                          if (chunks) console.log((chunks+' roundtrips').green);
                          console.log((secs+' secs').green);
                       };

                   if (promise==_||promise===false||promise===undefined) 
                   {
                      _ask(function () { console.log(promise); })();
                      return;
                   }

                   promise= promise || {};

                   if (promise.error)
                     promise.error(_ask(function (err) 
                     { 
                         if (!err) return;

                         if (err.code=='notfound')
                           console.log('no data found'.yellow);
                         else
                         if (err.code=='updatedsinceread')
                           console.log('The item is changed since you read it'.red);
                         else
                           console.log((err+'').red,err.stack); 
                     }));

                   if (promise.end)
                     promise.end(function () { end= true; doneres(); });

                   if (promise.count)
                     promise.count(_ask(function (count) { console.log(('\r'+count).green); elapsed(); }));
                        
                   if (promise.clean)
                     promise.clean(function (obj) {  console.log(util.inspect(obj,{ depth: null })); ask(); });
                   else
                   if (promise.result)
                   {
                     last= undefined;
                     promise.result(function (obj) { last= obj; _print(obj,function () { elapsed(); ask(); }); });
                   }
                   else
                   if (promise.results) 
                   {
                     last= [];
                     promise.results(function (items)
                     { 
                           chunks++;
                           printed= false;

                           last.push.apply(last,items);

                           _print(items,function () 
                           { 
                               printed= true;
                               doneres(); 
                           }); 
                     });
                   }
                   else
                   if (promise.success)
                     promise.success(_ask(function () { console.log('done!'.green); elapsed(); }));
                   else
                     _ask(function () { console.log(util.inspect(promise,{ depth: null })); })();
                }
                catch (ex)
                {
                   console.log('unknown command'.red,ex,ex.stack);
                   ask();
                }

                //rl.close();
             });
         })();
     });
   }
}];

if (argv.local)
  args.unshift({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') } });

dyngo.apply(null,args);
