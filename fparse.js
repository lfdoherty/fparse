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

var replayableBufw = require('./replayable_bufw')

function make(schemaStr){
	var schema = keratin.parse(schemaStr, []);
	makeFromSchema(schema);
}

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

		rstr += '\tvar e = {}\n'
		
		codes[objSchema.name] = objSchema.code
		names[objSchema.code] = objSchema.name

		if(objSchema.properties){
		
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
					sstr += '\tr.skipVarString()\n'
				}else if(type === 'boolean'){
					wstr += '\tw.putBoolean(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readBoolean()\n'
					sstr += '\tr.skipBoolean()\n'
				}else if(type === 'byte'){
					wstr += '\tw.putByte(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readByte()\n'
					sstr += '\tr.skipByte()\n'
				}else if(type === 'long'){
					wstr += '\tw.putLong(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readLong()\n'
					sstr += '\tr.skipLong()\n'
				}else if(type === 'binary'){
					wstr += '\tw.putBuffer(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readData()\n'
					sstr += '\tr.skipData()\n'
				}else if(type === 'real'){
					wstr += '\tw.putReal(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readReal()\n'
					sstr += '\tr.skipReal()\n'
				}else if(type === 'int'){
					wstr += '\tw.putInt(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readInt()\n'
					sstr += '\tr.skipInt()\n'
				}else if(type === 'uuid'){
					wstr += '\tw.putUuid(e.'+name+')\n';
					rstr += '\te.'+name+' = r.readUuid()\n'
					sstr += '\tr.skipUuid()\n'
				}else{
					_.errout('TODO: ' + JSON.stringify(p))
				}
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
		writersByCode: {},
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
		handle.writersByCode[objSchema.code] = handle.writers[objSchema.name]
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
		w.flushAndDie()	
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

exports.makeReusableSingleReader = function(){
	var r = rs.make()
	return r
}

exports.makeRs = rs.make

exports.makeWriteStream = function(fp, ws){
	var inFrame = false
	var fs = {}
	var w = new bufw.W(1024*1024, ws)
	Object.keys(fp.writers).forEach(function(key){
		
		//console.log('writing: ' + key)
		var writer = fp.writers[key]
		var code = fp.codes[key]
		_.assert(code < 255)//TODO support more than 255 message types?
		_.assert(code > 0)
		fs[key] = function(e){
			if(!inFrame){
				w.startLength()
				inFrame = true
			}
			w.putByte(code)
			writer(w, e)
		}
	})
	var handle = {
		shouldWriteFrame: function(){
			return inFrame && w.currentLength() > 0
		},
		forceBeginFrame: function(){
			if(!inFrame){
				w.startLength()
				inFrame = true
			}
		},
		endFrame: function(){
			w.endLength()
			w.flush()
			inFrame = false
		},
		fs: fs,
		end: function(){
			w.flush()
		},
		writer: w
	}
	return handle
}

exports.makeReplayableWriteStream = function(fp, ws){
	
	var fs = {}
	
	var w = new replayableBufw.W(1024*1024, ws)

	var hasWritten = false

	var inFrame = false
	
	var ackFramePushedLast = false
	
	var frameCount = 0
	var frameLengths = []
	Object.keys(fp.writers).forEach(function(key){
		var writer = fp.writers[key]
		var code = fp.codes[key]
		_.assert(code < 255)//TODO support more than 255 message types?
		_.assert(code > 0)
		fs[key] = function(e){
			if(!inFrame){
				beginFrame()
			}
			hasWritten = true
			w.putByte(code)
			writer(w, e)
		}
	})
	
	function beginFrame(){
		if(!inFrame){
			w.putByte(1)
			w.startLength()
			hasWritten = false
			inFrame = true
		}
	}
	
	var handle = {
		writeAck: function(v){
			if(inFrame){
				handle.endFrame()
			}
			w.putByte(2)
			w.putInt(v)
			if(frameLengths.length === 0){
				ackFramePushedLast = true
				frameLengths.push(5)
			}else{
				frameLengths[frameLengths.length-1] += 5
			}
			w.flush()
		},
		forceBeginFrame: function(){
			beginFrame()
		},
		shouldWriteFrame: function(){
			return inFrame && w.currentLength() > 0
		},
		endFrame: function(){
			if(!inFrame) _.errout('cannot end what was never begun')
			var len = w.endLength()
			_.assertInt(len)
			_.assert(len > 0)
			if(ackFramePushedLast){
				_.assertLength(frameLengths, 1)
				frameLengths[0] += len+5
				ackFramePushedLast = false
			}else{
				frameLengths.push(len+5)
			}
			w.flush()
			++frameCount
			inFrame = false
			return true
		},
		hasWritten: function(){
			return hasWritten
		},
		fs: fs,
		end: function(){
			w.flush()
		},
		writer: w,
		getFrameCount: function(){
			return frameCount
		},
		discardReplayableFrames: function(manyFrames){
			_.assert(manyFrames > 0)
			_.assert(manyFrames <= frameLengths.length)
			
			var totalLength = 0
			for(var i=0;i<manyFrames;++i){
				_.assert(frameLengths[i] > 5)
				totalLength += frameLengths[i]
			}
			frameLengths = frameLengths.slice(manyFrames)
			w.discardReplayable(totalLength)
		},
		replay: function(){
			w.replay()
		}
	}
	return handle
}

exports.makeReadStream = function(fp, readers){
	
	var r = rs.make()
	var b
	var frameCount = 0

	var amountWaitingFor
	var bufs
	var gotSoFar
	
	function put(buf){
		if(b === undefined){
			b = buf
			gotSoFar = buf.length
		}else{
			gotSoFar += buf.length
			if(amountWaitingFor > gotSoFar){
				if(bufs){
					bufs.push(buf)
				}else{
					bufs = [b, buf]
				}
				return
			}
			if(bufs){
				bufs.push(buf)
				b = Buffer.concat(bufs)
				bufs = undefined
			}else{
				b = Buffer.concat([b, buf])
			}
		}
		
		var off = 0
		
		while(true){

			amountWaitingFor = 0
			
			if(b.length < off) break;
	
			var waitingFor = bin.readInt(b, off)
			if(waitingFor === 0){
				if(b.length < off+4){
					break;
				}
				++frameCount
				off += 4
				continue
			}
			var end = 4+waitingFor+off
			if(end > b.length){
				amountWaitingFor = waitingFor+4
				break
			}

			++frameCount
			off+=4

			r.put(b, off, end)
			
			while(r.getOffset() < end){
			
				var code = r.s.readByte()
				_.assert(code > 0)
				var name = fp.names[code]
				var e = fp.readersByCode[code](r.s)
				readers[name](e)
			}

			if(b.length === end){
				b = undefined
				gotSoFar = 0
				return
			}			
			off = end
		}
		b = b.slice(off)
		gotSoFar = b.length
	}
	
	put.getFrameCount = function(){
		return frameCount
	}
	
	return put
}

