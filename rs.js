var _ = require('underscorem');
var bin = require('./bin');

var ccc = console.log

function makeReadState(){

	var cur
	var off
	var curEnd
	
	var s = {
		readInt: function(){
			var v = bin.readInt(cur, off);
			off+=4;
			return v;
		},
		skipInt: function(){
			off += 4
		},
		readString: function(len){
			//_.assertInt(len)
			var v = cur.toString('utf8', off, off+len);
			off += len;
			return v;
		},
		readData: function(){
			//_.assertInt(len)
			var len = s.readInt()
			var v = cur.slice(off, off+len)
			off += len;
			return v;
		},
		skipData: function(){
			var len = s.readInt()
			off += len
		},
		readVarData: function(){
			var len = s.readLength()
			var v = cur.slice(off, off+len)
			off += len;
			return v;
		},
		readVarString: function(){
			var len = s.readLength()
			return s.readString(len)
		},
		skipVarString: function(){
			var len = s.readLength()
			off+=len
		},
		readReal: function(){
			var len = s.readLength()
			return Number(s.readString(len))
		},
		skipReal: function(){
			var len = s.readLength()
			off += len
		},
		readByte: function(){
			var v = cur[off];
			++off
			return v;
		},
		skipByte: function(){
			++off
		},
		readLong: function(){
			var v = bin.readLong(cur, off);
			off+=8;
			return v;
		},
		skipLong: function(){
			off+=8
		},
		readBoolean: function(){
			var b = cur[off];
			++off;
			//if(b !== 0 && b !== 1) console.log('b: ' + b)
			//_.assert(b === 0 || b === 1);
			return b === 1;
		},
		skipBoolean: function(){
			++off
		},
		readLength: function(){
			var count = s.readByte();
			if(count === 255){
				count += s.readInt();
			}
			return count;
		}
	}
	s.readVarUint = s.readLength
	
	return {
		s: s,
		assertEmpty: function(){
			_.assertEqual(off, curEnd);
		},
		put: function(buf, start, end){
			if(start === undefined) start = 0
			if(end === undefined) end = buf.length
			cur = buf;
			curEnd = end
			off = start;
		},
		getOffset: function(){
			return off
		}
	}
}
exports.make = makeReadState;
