var crypto = require('crypto');

module.exports = {
    toHexString : function (byteArray) {
        return Array.from(byteArray, function(byte) {
            return ('0' + (byte & 0xFF).toString(16)).slice(-2);
        }).join('')
    },

    cryptoRandomNumber : function(minimum, maximum){
        var distance = maximum-minimum;

        if(minimum>=maximum){
            console.log('Minimum number should be less than maximum');
            return false;
        } else if(distance>281474976710655){
            console.log('You can not get all possible random numbers if range is greater than 256^6-1');
            return false;
        } else if(maximum>Number.MAX_SAFE_INTEGER){
            console.log('Maximum number should be safe integer limit');
            return false;
        } else {
            var maxBytes = 6;
            var maxDec = 281474976710656;

            var randbytes = parseInt(crypto.randomBytes(maxBytes).toString('hex'), 16);
            var result = Math.floor(randbytes / maxDec * (maximum - minimum + 1) + minimum);

            if (result > maximum) {
                result = maximum;
            }
            return result;
        }
    },
};