/* global require */
/* global module */
/* global process */

var weblogMysql = function(setup) {
  setup.host  = setup.host ? setup.host : require('ip').address()
  setup.topic = setup.domain+'.'+setup.host+'.'+setup.service

  var _ = require('lodash')
  var when = require('when')
  var knex = require('knex')({
    client: 'mysql',
    debug: false,
  //  debug: true,
    connection: setup.dbconnection
  })
  var autobahn = require('autobahn')

  var connection = new autobahn.Connection({
    url: process.argv[2] || 'ws://127.0.0.1:8080/ws',
    realm: process.argv[3] || 'weblog'
  })

  var main = function(session) {
    session.subscribe('discover', function() {
      session.publish('announce', [_.pick(setup, 'domain', 'host', 'service', 'topic')])
    })

    session.register(setup.topic+'.header', function() {
      return setup.headers
    })

    session.register(setup.topic+'.reload', function(args) {
      var d = when.defer()
      var controls = args[0]
//console.log('JSON3', JSON.stringify(controls.header, null, 2));
      if (controls.offset < 0) controls.offset = 0
      var table = controls.header
      if (table.select) table.view = knex.raw(table.select).wrap('(', ') t1')
      var wherearr = []
      wherearr.push(table.where)
      if (controls.begin)  wherearr.push(table.fields[controls.rangefield]+' >= \''+controls.begin+'\'')
      if (controls.end)    wherearr.push(table.fields[controls.rangefield]+' <= \''+controls.end+'\'')
      if (controls.filter) wherearr.push(table.fields[controls.filterfield]+' LIKE \'%'+controls.filter+'%\'')
      var where = wherearr.join(' AND ')
      knex.select(table.fields).from(table.view).whereRaw(where).orderBy(table.orderby).limit(controls.count).offset(controls.offset)
      .then(function(rows) {
        var res = []
        _.each(rows, function(row) { res.push(_.values(row)) })
        d.resolve(res)
      })
      return d.promise
    })
  }

  connection.onopen = main

  connection.open()
}

module.exports = weblogMysql
