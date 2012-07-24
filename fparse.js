"use strict";
/*
var old = console.log
console.log = function(msg){
	msg += ''
	if(msg.length > 5000) throw new Error('too long')
	old(msg)
}*/

var keratin = require('keratin');
var _ = require('underscorem');
var rs = require('./rs')
var bufw = require('./bufw')

function make(schemaStr){
	var schema = keratin.parse(schemaStr, []);
	makeFromSchema(schema);
}

function makeFromSchema(schema){	
	
	var readers = {}
	var writers = {}
	var codes = {}
	var names = {}
	_.each(schema._byCode, function(objSchema){
		var wstr = 'if(w === undefined) throw new Error(\'parameter 0,  "w" is undefined\');if(e === undefined) throw new Error(\'parameter 1,  "e" is undefined\');'
		var rstr = 'if(r === undefined) throw new Error(\'parameter 0,  "r" is undefined\');var e = {};'
		codes[objSchema.name] = objSchema.code
		names[objSchema.code] = objSchema.name
		var c = Object.keys(objSchema.propertiesByCode)
		c = c.sort()
		c.forEach(function(codeStr){
			var p = objSchema.propertiesByCode[codeStr]
			var type = p.type.primitive
			var name = p.name
			var code = p.code
			
			if(type === 'string'){
				wstr += 'w.putString(e.'+name+');'
				rstr += 'e.'+name+' = r.readVarString();'
			}else if(type === 'boolean'){
				wstr += 'w.putBoolean(e.'+name+');';
				rstr += 'e.'+name+' = r.readBoolean();'
			}else if(type === 'byte'){
				wstr += 'w.putByte(e.'+name+');';
				rstr += 'e.'+name+' = r.readByte();'
			}else if(type === 'long'){
				wstr += 'w.putLong(e.'+name+');';
				rstr += 'e.'+name+' = r.readLong();'
			}else if(type === 'binary'){
				wstr += 'w.putVarData(e.'+name+');';
				rstr += 'e.'+name+' = r.readVarData();'
			}else if(type === 'real'){
				wstr += 'w.putReal(e.'+name+');';
				rstr += 'e.'+name+' = r.readReal();'
			}else if(type === 'int'){
				wstr += 'w.putInt(e.'+name+');';
				rstr += 'e.'+name+' = r.readInt();'
			}else{
				_.errout('TODO: ' + JSON.stringify(p))
			}
		})
		rstr += 'return e;'
		//console.log(wstr)
		var fw = new Function('w', 'e', wstr)
		var fr = new Function('r', 'e', rstr)
		readers[objSchema.name] = fr
		writers[objSchema.name] = fw
	})
	
	
	return {
		readers: readers,
		writers: writers,
		codes: codes,
		names: names
	}
}

exports.make = make;
exports.makeFromSchema = makeFromSchema;
exports.makeWriter = function(ws){
	return new bufw.W(1024*1024, ws)
}
exports.makeSingleBufferWriter = function(){
	var buf
	var ws = {
		write: function(b){
			buf = b
		}
	}
	var w = new bufw.W(1024*1024, ws)
	w.delay()
	w.finish = function(){
		w.resume()
		w.flush()	
		//_.assertBuffer(buf)
		if(buf === undefined) buf = new Buffer(0)
		return buf
	}
	return w
}
exports.makeSingleReader = function(buf){
	_.assertBuffer(buf)
	var r = rs.make()
	r.put(buf)
	return r.s
}


exports.makeRs = rs.make



