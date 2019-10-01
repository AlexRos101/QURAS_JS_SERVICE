var define = require('node-constants')(exports);

define("CONSOLELOG_FLAG", 1);

define("NORMAL_DELAY", 1000 * 5);
define("FAST_DELAY", 100 * 1);

define("MYSQL_OPTIONS", {
	connectionLimit : 100,
	host	: 'localhost',
	user	: 'root',
	password: '',
	database: 'quras_db',
	debug	: false
});

define("ERR_CONSTANTS",{
	connection_err		: 0,
	success				: 1,
	err_db_status 		: 2,
	err_rpc_server		: 3,
	err_no_blockNum 	: 4,
});