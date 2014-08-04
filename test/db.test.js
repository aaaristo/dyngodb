var should= require('chai').should(),
    assert= require('chai').assert,
    AWS = require('aws-sdk'),
    _= require('underscore'),
    dyngo=  require('../index.js');

const noerr= function (done)
      {
         return function (err)
         {
             should.not.exist(err);
             done();
         };
      },
      accept= function (code,done)
      {
         return function (err)
         {
           if (err.code==code)
             done(); 
           else
             done(err);
         };
      };


describe('database',function ()
{
       var db;

       before(function (done)
       {
            dyngo({ dynamo: { endpoint: new AWS.Endpoint('http://localhost:8000') }, hints: false },
            function (err,_db)
            {
               db= _db; 
               done();
            });
       });

       beforeEach(function (done)
       {
            db.test.remove().success(done)
                            .error(done); 
       });

       it('Can connect', function (done)
       {
          should.exist(db);
          done();
       });

       it('test table should be empty', function (done)
       {
          db.test.find().results(function (items)
          {
              items.length.should.equal(0);
              done();
          })
          .error(noerr(done));
       });

       describe('save',function ()
       {
           it('Can insert a new object, and then find it', function (done)
           {
                var _noerr= noerr(done);

                db.test.save({ somedata: 'ok' })
                       .success(function ()
                       {
                             db.test.findOne({ somedata: 'ok' })
                                    .result(function (obj)
                                    {
                                          obj.somedata.should.equal('ok'); 
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object, and have it back', function (done)
           {
                var _noerr= noerr(done);

                db.test.save({ somedata: 'parent', child: { somedata: 'child' } })
                       .success(function ()
                       {
                             db.test.findOne({ somedata: 'parent' })
                                    .result(function (obj)
                                    {
                                          obj.somedata.should.equal('parent'); 
                                          obj.child.somedata.should.equal('child'); 
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object, and then remove the child', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentrchild', child: { somedata: 'child' } };

                db.test.save(par)
                       .success(function ()
                       {
                             delete par.child;

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentrchild'); 
                                                      should.not.exist(obj.child); 
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object, and then change the type of the child', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentrchild', child: { somedata: 'child' } };

                db.test.save(par)
                       .success(function ()
                       {
                             par.child= 'child';

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentrchild'); 
                                                      obj.child.should.equal('child');
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object array, and have it back', function (done)
           {
                var _noerr= noerr(done);

                db.test.save({ somedata: 'parentarr', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] })
                       .success(function ()
                       {
                             db.test.findOne({ somedata: 'parentarr' })
                                    .result(function (obj)
                                    {
                                          obj.somedata.should.equal('parentarr'); 
                                          obj.childs.length.should.equal(3); 
                                           _.pluck(obj.childs,'somedata').should.contain('child1');
                                           _.pluck(obj.childs,'somedata').should.contain('child2');
                                           _.pluck(obj.childs,'somedata').should.contain('child3');
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object array, and then update the parent without loosing the child object array', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentrarrupd', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] };

                db.test.save(par)
                       .success(function ()
                       {
                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentrarrupd'); 
                                                      should.exist(obj.childs); 
                                                      obj.childs.length.should.equal(3); 
                                                       _.pluck(obj.childs,'somedata').should.contain('child1');
                                                       _.pluck(obj.childs,'somedata').should.contain('child2');
                                                       _.pluck(obj.childs,'somedata').should.contain('child3');
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object array, and then remove the child object array', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentrarrrm', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] };

                db.test.save(par)
                       .success(function ()
                       {
                             delete par.childs;

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentrarrrm'); 
                                                      should.not.exist(obj.childs); 
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object array, and then change the type of the child array', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentcarr', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] };

                db.test.save(par)
                       .success(function ()
                       {
                             par.childs= ['child1','child2','child3'];

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentcarr'); 
                                                      obj.childs.length.should.equal(3); 
                                                      obj.childs.should.contain('child1');
                                                      obj.childs.should.contain('child2');
                                                      obj.childs.should.contain('child3');
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with a child object array, and remove an element from the array', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentcarr', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] };

                db.test.save(par)
                       .success(function ()
                       {
                             par.childs.splice(2,1);

                             db.test.save(par)
                                    .success(function ()
                                    {
                                         db.test.findOne({ _id: par._id })
                                                .result(function (obj)
                                                {
                                                      obj.somedata.should.equal('parentcarr'); 
                                                      obj.childs.length.should.equal(2); 
                                                      _.pluck(obj.childs,'somedata').should.contain('child1');
                                                      _.pluck(obj.childs,'somedata').should.contain('child2');
                                                      done();
                                                })
                                                .error(_noerr);
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('Can insert an object with an independent child object array, and remove an element from the array', function (done)
           {
                var _noerr= noerr(done);

                var par= { somedata: 'parentcarr', childs: [{ somedata: 'child1' },{ somedata: 'child2' },{ somedata: 'child3' }] };

                db.test.save(par.childs)
                       .success(function ()
                       { 
                            db.test.save(par)
                                   .success(function ()
                                   {
                                         par.childs.splice(1,1);

                                         db.test.save(par)
                                                .success(function ()
                                                {
                                                     db.test.findOne({ _id: par._id })
                                                            .result(function (obj)
                                                            {
                                                                  obj.somedata.should.equal('parentcarr'); 
                                                                  obj.childs.length.should.equal(2); 
                                                                  _.pluck(obj.childs,'somedata').should.contain('child1');
                                                                  _.pluck(obj.childs,'somedata').should.contain('child3');
                                                                  done();
                                                            })
                                                            .error(_noerr);
                                                })
                                                .error(_noerr);
                                   })
                                   .error(_noerr);
                       })
                       .error(_noerr);
           });

           it('supports Date objects', function (done)
           {
                var _noerr= noerr(done), d= new Date();

                var par= { val: d };

                db.test.save(par)
                       .success(function ()
                       {
                             db.test.findOne({ _id: par._id })
                                    .result(function (obj)
                                    {
                                          should.exist(obj.val);
                                          obj.val.should.equal(d.toISOString()); 
                                          done();
                                    })
                                    .error(_noerr);
                       })
                       .error(_noerr);
           });
       });

       describe('remove',function ()
       {
           it('Can delete objects found by _id, with client side filtering', function (done)
           {
                // if filter attributes are not included in projection by the finder
                // when it can't filter them, this test should fail
                db.test.save({ _id: 'toremove', uid: 'andrea' })
                       .success(function ()
                       {
                             db.test.remove({ _id: 'toremove', uid: 'andrea' })
                                    .success(function ()
                                    {
                                        db.test.findOne({ _id: 'toremove', uid: 'andrea' }) 
                                               .result(should.not.exist)
                                               .error(accept('notfound',done));
                                    })
                                    .error(done);
                       })
                       .error(done);
           });
       });
});
