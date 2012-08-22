"use strict";
/*
var old = console.log
console.log = function(msg){
	msg += ''
	if(msg.length > 5000) throw new Error('too long')
	old(msg)
}*/

var fs = require('fs')

var bin = require('./bin')

var keratin = require('keratin');
var _ = require('underscorem');
var rs = require('./rs')
var bufw = require('./bufw')

function make(schemaStr){
	var schema = keratin.parse(schemaStr, []);
	makeFromSchema(schema);
}

//var count = 0

function makeWriter(name, fw){
	return function(w,e){
		try{	
			fw(w,e)
		}catch(e2){
			_.errout('write failed for object(' + name + '): ' + JSON.stringify(e) + '\n' + e2)
		}
	}
}

function makeFromSchema(schema){	
	
	var readers = {}
	var readersByCode = {}
	var writers = {}
	var codes = {}
	var names = {}

	var wstr = ''
	var rstr = ''
	var sstr = ''
		
	wstr += 'var writers = exports.writers = {}\n'
	rstr += 'var readers = exports.readers = {}\n'
	sstr += 'var skippers = exports.skippers = {}\n'
	
	var keys = Object.keys(schema._byCode)
	for(var i=0;i<keys.length;++i){
		var objCodeStr = keys[i]
		var objSchema = schema._byCode[objCodeStr]
		
		
		
		wstr += 'writers.' + objSchema.name + ' = function(w, e){\n'
		rstr += 'readers.' + objSchema.name + ' = function(r){\n'
		sstr += 'skippers.' + objSchema.name + ' = function(r){\n'

		//wstr += '\tif(w === undefined) throw new Error(\'parameter 0,  "w" is undefined\')\n'
		//wstr += '\tif(e === undefined) throw new Error(\'parameter 1,  "e" is undefined\')\n'

		//rstr += '\tif(r === undefined) throw new Error(\'parameter 0,  "r" is undefined\')\n'//var e = {};'
		rstr += '\tvar e = {}\n'
		
		codes[objSchema.name] = objSchema.code
		names[objSchema.code] = objSchema.name
		var c = Object.keys(objSchema.propertiesByCode)
		c = c.sort()
		for(var j=0;j<c.length;++j){
			var codeStr = c[j]
			var p = objSchema.propertiesByCode[codeStr]
			var type = p.type.primitive
			var name = p.name
			var code = p.code
			
			if(type === 'string'){
				wstr += '\tw.putString(e.'+name+')\n'
				rstr += '\te.'+name+' = r.readVarString()\n'
				sstr += '\tr.readVarString()\n'
			}else if(type === 'boolean'){
				wstr += '\tw.putBoolean(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readBoolean()\n'
				sstr += '\tr.readBoolean()\n'
			}else if(type === 'byte'){
				wstr += '\tw.putByte(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readByte()\n'
				sstr += '\tr.readByte()\n'
			}else if(type === 'long'){
				wstr += '\tw.putLong(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readLong()\n'
				sstr += '\tr.readLong()\n'
			}else if(type === 'binary'){
				wstr += '\tw.putVarData(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readVarData()\n'
				sstr += '\tr.readVarData()\n'
			}else if(type === 'real'){
				wstr += '\tw.putReal(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readReal()\n'
				sstr += '\tr.readReal()\n'
			}else if(type === 'int'){
				wstr += '\tw.putInt(e.'+name+')\n';
				rstr += '\te.'+name+' = r.readInt()\n'
				sstr += '\tr.readInt()\n'
			}else{
				_.errout('TODO: ' + JSON.stringify(p))
			}
		}
		rstr += '\treturn e;\n'
		rstr += '}\n'
		
		wstr += '}\n'
		
		sstr += '}\n'
	}
	
	var tempFilePath = process.cwd()+'/'+'.temp_schema_file.'+Math.random()+'.tempschemafile.js'
	var fullStr = wstr + '\n' + rstr + '\n' + sstr
	fs.writeFileSync(tempFilePath, fullStr)
	
	var tempResult = require(tempFilePath)
	
	fs.unlinkSync(tempFilePath)
	
	var handle = {
		readersByCode: {},
		skippersByCode: {},
		codes: codes,
		names: names
	}
	handle.readers = tempResult.readers
	handle.writers = tempResult.writers
	handle.skippers = tempResult.skippers
	
	for(var i=0;i<keys.length;++i){
		var objCodeStr = keys[i]
		var objSchema = schema._byCode[objCodeStr]
		handle.readersByCode[objSchema.code] = handle.readers[objSchema.name]
		handle.skippersByCode[objSchema.code] = handle.skippers[objSchema.name]
	}
	return handle
}

exports.make = make;
exports.makeFromSchema = makeFromSchema;
exports.makeWriter = function(ws){
	return new bufw.W(1024*1024, ws)
}
exports.makeTemporaryBufferWriter = function(bufSize){

	var dummyWs = {
		write: function(){_.errout('error')}
	}
	var nkw = new bufw.W(bufSize, dummyWs)
	nkw.delay()

	var handle = {
		get: function(){
			var b = nkw.getBackingBuffer().slice(0, nkw.getCurrentOffset())
			nkw.cancel()
			nkw.delay()
			return b
		},
		w: nkw
	}
	return handle
}
exports.makeSingleBufferWriter = function(bufSize){
	var buf
	var ws = {
		write: function(b){
			buf = b
		}
	}
	bufSize = bufSize || 1024*1024
	
	var w = new bufw.W(bufSize, ws)
	w.delay()
	w.finish = function(){
		//w.resume()
		w.flushAndDie()	
		//_.assertBuffer(buf)
		//if(buf === undefined) buf = new Buffer(0)
		_.assertBuffer(buf)
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

exports.makeWriteStream = function(fp, ws){
	var fs = {}
	var w = new bufw.W(1024*1024, ws)
	Object.keys(fp.writers).forEach(function(key){
		var writer = fp.writers[key]
		var code = fp.codes[key]
		_.assert(code < 255)//TODO support more than 255 message types?
		_.assert(code > 0)
		fs[key] = function(e){
			w.putByte(code)
			writer(w, e)
		}
	})
	var handle = {
		beginFrame: function(){
			w.startLength()
		},
		endFrame: function(){
			w.endLength()
			w.flush()
		},
		fs: fs,
		end: function(){
			w.flush()
		},
		writer: w
	}
	return handle
}
exports.makeReadStream = function(fp, readers){
	var r = rs.make()
	var b
	function put(buf){
		if(b === undefined){
			b = buf
		}else{
			b = Buffer.concat([b, buf])
		}
		
		while(true){

			var waitingFor = bin.readInt(b, 0)
			var end = waitingFor+4
			if(end > b.length) return

			r.put(b, 4, end)
			
			while(r.getOffset() < end){
			
				var code = r.s.readByte()
				//console.log(r.getOffset() + ' ' + frame.length + ' code: ' + code)
				_.assert(code > 0)
				var name = fp.names[code]
				var e = fp.readers[name](r.s)
				readers[name](e)
			}

			if(b.length === end){
				b = undefined
				return
			}
			
			b = b.slice(end)
		}
	}
	
	return put
}

