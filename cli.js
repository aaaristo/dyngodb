#!/usr/local/bin/node

const NOPE= function (){};

var dyngo= require('./index'),
    async= require('async'),
    fs= require('fs'),
    csv = require('csv'),
    xlsx = require('./lib/xlsx'),
    carrier= require('carrier'),
    util= require('util'),
    readline= require('readline'),
    _= require('underscore'),
    path= require('path').join,
    colors = require('colors'),
    GSON = require('gson'),
    coffee= require('coffee-script'),
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
      _gson= function (path,content)
      {
          try
          {
              if (!content)
                return GSON.parse(fs.readFileSync(path,'utf8'));
              else
              {
                fs.writeFileSync(path,GSON.stringify(content),'utf8')
                return { success: function (fn) { process.nextTick(fn); } };
              }
          }
          catch (ex)
          {
              console.log((ex+'').red);
          }
      },
      _toJSON= function (fields,_transformFnc)
      {
          var r= [];

          this.forEach(function (row,idx)
          {
              var obj= {},
                  _val= function (field,value)
                  {
                     var r,
                         path= field.split('.'),
                         current= obj;

                     for (var i=0;i<path.length-1;i++)
                        current= current[path[i]]= current[path[i]] || {};

                     current[path[path.length-1]]= value;
                  };

              for (var i=0;i<fields.length;i++)
                 _val(fields[i],row[i]);

              if (_transformFnc)
                obj= _transformFnc(obj,idx);

              if (Array.isArray(obj))
                r.concat(obj);
              else
              if (obj)
                r.push(obj);
          });

          return r;
      },
      _csv= function (dyn, path, opts, cols, tfnc)
      {
         var raster= [],
             promise= dyn.promise(['end','results']),
             count= 0;

         raster.toJSON= _toJSON;

         csv()
            .from.path(path, opts)
            .on('record',function (row,index)
            {
                 process.stdout.write(('\r'+(count++)).yellow);
                 raster.push(row);
            })
            .on('end',function (count)
            {
                 console.log(('\r'+count).green);
                 promise.trigger.results(raster.toJSON(cols,tfnc));
                 promise.trigger.end();
            })
            .on('error', promise.error);

         return promise;
      },
      _xlsx= function (src)
      {
            var b64= fs.readFileSync(src, "base64"),
                workbook= xlsx.decode(b64);

            workbook.sheet= function (name)
            { 
               var worksheet;

               this.worksheets.forEach(function (s)
               {
                  if (s.name==name)
                  {
                      worksheet= s;
                      return false;
                  }
               });

               return worksheet;
            }

            workbook.worksheets.forEach(function (s)
            {
               s.toJSON= function (fields,_transformFnc)
               {
                  var r= [];

                  this.data.forEach(function (row,idx)
                  {
                      var obj= {},
                          _nnan= function (v) { return v===0 ? v : (!!v ? v : '') }, 
                          _value= function (value)
                          {
                             if (value)
                               return _nnan(value.value);
                             else
                               return '';
                          },
                          _val= function (field,value)
                          {
                             var r,
                                 path= field.split('.'),
                                 current= obj;

                             for (var i=0;i<path.length-1;i++)
                                current= current[path[i]]= current[path[i]] || {};

                             current[path[path.length-1]]= _value(value);
                          };

                      for (var i=0;i<fields.length;i++)
                         _val(fields[i],row[i]); 

                      if (_transformFnc)
                        obj= _transformFnc(obj,idx);
                      
                      if (Array.isArray(obj))
                        r.concat(obj);
                      else
                      if (obj)
                        r.push(obj);
                  });

                  return r;
               }
            });

            return workbook;
      },
      _eval= function (cmd,db,last,tx)
      {
        var __csv= function (path, opts, cols, tfnc) { var args= Array.prototype.slice.apply(arguments); args.unshift(db._dyn); return _csv.apply(null,args); };

        try
        {
          return eval('(function (db,tx,fs,last,_,json,gson,csv,xlsx,argv){ return '+cmd+'; })')(db,tx,fs,last,_,_json,_gson,__csv,_xlsx,argv);
        }
        catch (ex)
        {
          if (ex instanceof SyntaxError||ex instanceof ReferenceError)
           return coffee.eval('((global,db,tx,fs,last,_,json,gson,csv,xlsx,argv) -> '+cmd+')')(global,db,tx,fs,last,_,_json,_gson,__csv,_xlsx,argv);
          else
            throw ex;
        }
      },
      _dobatch= function (db,lines,done)
      {
         var last, tx;

         return function ()
                {
                        async.forEachSeries(lines,
                        function (cmd,done)
                        {
                           console.log(cmd);

                           var promise= _eval(cmd,db,last,tx);

                           if (promise==undefined||!(promise.result||promise.success||promise.error||promise.notfound))
                             done();
                           else
                           {
                               if (promise.notfound)
                                 promise.notfound(function ()
                                 {
                                     last= undefined;
                                     done();
                                 });
                               else
                               if (promise.error)
                                 promise.error(function (err)
                                 {
                                     console.log((err+'').red,err.stack); 
                                     done();
                                 });

                               if (promise.transaction)
                                 promise.transaction(function (_tx)
                                 {
                                     tx= _tx;
                                     console.log(('tx: '+tx._id).green);
                                     done();
                                 });
                               else
                               if (promise.result)
                                 promise.result(function (res)
                                 {
                                     last= res;
                                     done();
                                 });
                               else
                               if (promise.success)
                                 promise.success(function ()
                                 {
                                     console.log('done!'.green);
                                     done();
                                 });
                           }


                        },done);
                };
      },
      _doinput= function (db,cb)
      {
         var _lines= [];

         carrier.carry(process.stdin, function (line)
         {
                _lines.push(line);
         },'utf8');

         process.stdin.on('end',function ()
         {
             process.nextTick(function ()
             {
                 _dobatch(db,_lines,
                 function (err)
                 {
                      if (err)
                      {
                        console.log(err.message.red,err.stack);
                        process.exit(1); 
                      }
                      else
                        process.exit(0); 
                 })();
             });
         });

         process.stdin.resume();

         // if we have no input go to interactive mode
         setTimeout(function () { if (_lines.length==0) cb(); },100);
      },
      _dorc= function (db,cb)
      {
          var rcFile= path(getUserHome(),'.dyngorc'),
              localRcFile= '.dyngorc',
              _file= function (f, done)
              {
                 var _lines= [];

                 if (fs.existsSync(f))  
                 {
                    console.log(('executing '+f+'...').green);
                    var rstream= fs.createReadStream(f, { encoding: 'utf8' });

                    rstream.on('end',function ()
                    {
                       process.nextTick(function ()
                       { 
                        _dobatch(db,_lines,done)();
                       });
                    });

                    carrier.carry(rstream,function (line)
                    {
                        _lines.push(line);
                    });
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
      },
      _collect= function (consume)
      {
          return function (cons) 
          { 
            _.keys(cons).forEach(function (table)
            {
                var c, tcons= cons[table];

                if (!(c=consume[table]))
                  c= consume[table]= { read: 0, write: 0 };

                c.read+= tcons.read;
                c.write+= tcons.write;
            }); 
          };
      };

process.on('exit', saveHistory);
process.on('SIGINT', function () { saveHistory(); process.exit(0); });
process.stdin.pause();

var args= [function (err,db)
{
   var last, tx;

   if (err)
     console.log(err);
   else
   {

     _dorc(db,function ()
     {
           _doinput(db,function()
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
                         if (obj._old||(obj[0]&&obj[0]._old))
                             db.cleanup(obj).clean(function (obj)
                             {
                                console.log(util.inspect(obj,{ depth: null }));
                                cb();
                             });
                         else
                         {
                             console.log(util.inspect(obj,{ depth: null }));
                             cb();
                         }
                     };

                 rl.question('> ', function (answer) 
                 {

                    if (!answer) { ask(); return; };

                    if (answer.indexOf('clean ')==0)
                    {
                       var target= answer.substring(6);

                       answer= 'db.cleanup('+target+')';
                    }
                    
                    if (answer.indexOf('show collections') > -1)
                    { 
                       _.filter(_.keys(db),function (key) { return !!db[key]&&!!db[key].find; }).forEach(function (c) { console.log(c); });
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
                    else
                    if (answer=='exit')
                    {
                       process.exit(0);
                       return;
                    }

                    try
                    {
                       var time= process.hrtime(),
                           promise= _eval(answer,db,last,tx),
                           end,
                           printed,
                           chunks= 0,
                           consume= { read: 0, write: 0 },
                           _doneres= function () { elapsed(); ask(); },
                           doneres= _.wrap(_doneres,function (done) {  if (printed&&end) done(); }),
                           elapsed= function ()
                           {
                              var diff= process.hrtime(time),
                                  secs= (diff[0]*1e9+diff[1])/1e9;

                              _.keys(consume).forEach(function (table)
                              {
                                  var tcons= consume[table], s= secs<1 ? 1 : secs;

                                  if (tcons.read)
                                    console.log(('consumed read capacity['+table+']: '+tcons.read+' ('+(tcons.read/s)+' read/sec)').green);

                                  if (tcons.write)
                                    console.log(('consumed write capacity['+table+']: '+tcons.write+' ('+(tcons.write/s)+' write/sec)').green);
                              });

                              if (chunks) console.log((chunks+' roundtrips').green);
                              console.log((secs+' secs').green);
                           };

                       if (promise==_||promise===false||promise===undefined||promise.createCollection) 
                       {
                          _ask(function () { console.log(promise); })();
                          return;
                       }

                       promise= promise || {};

                       if (promise.consumed)
                         promise.consumed(_collect(consume));

                       if (promise.error)
                         promise.error(_ask(function (err) 
                         { 
                             if (!err) return;

                             if (err.code=='notfound')
                               console.log('no data found'.yellow);
                             else
                             if (err.code=='exists')
                               console.log('The item already exists'.red);
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
                            
                       if (promise.transaction)
                         promise.transaction(function (_tx) { tx= _tx; console.log(('tx: '+tx._id).green); elapsed(); ask(); });
                       else
                       if (promise.committed)
                         promise.committed(function () { tx= undefined; console.log('transaction committed'.green); elapsed(); ask(); });
                       else
                       if (promise.rolledback)
                         promise.rolledback(function (competing) { tx= undefined; if (competing) console.log('transaction rolled back'.red); else console.log('transaction rolled back'.green); elapsed(); ask(); });
                       else
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
     });
   }
}];

if (argv.local) 
  args.unshift({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') } });

dyngo.apply(null,args);
