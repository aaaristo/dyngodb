#!/usr/local/bin/node

var dyngo= require('./index'),
    async= require('async'),
    fs= require('fs'),
    util= require('util'),
    readline= require('readline'),
    _= require('underscore'),
    path= require('path').join,
    colors = require('colors');

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

dyngo(function (err,db)
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
                 last= obj;
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

            try
            {
               var time= process.hrtime(),
                   promise= eval('(function (db,last,_,json){ return '+answer+'; })')(db,last,_,_json),
                   elapsed= function ()
                   {
                      var diff= process.hrtime(time),
                          secs= (diff[0]*1e9+diff[1])/1e9;

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

               if (promise.count)
                 promise.count(_ask(function (count) { console.log(count); elapsed(); }));
               
               if (promise.clean)
                 promise.clean(function (obj) {  console.log(util.inspect(obj,{ depth: null })); ask(); });
               else
               if (promise.result)
                 promise.result(function (obj) { _print(obj,function () { elapsed(); ask(); }); });
               else
               if (promise.results)
                 promise.results(function (items) { _print(items,function () { elapsed(); ask(); }); });
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
   }
});
