
/**
 ** Change to the examples directory so this program can run as a service.
 **/
process.chdir(__dirname);

// config
var env = process.env.NODE_ENV || "development";
var config = require('./config.json')[env];

var fs = require ("fs");
var service = require ("os-service");
var mysql = require('mysql');
var async = require('async');
var crypto = require("crypto");
var common = require("./common");
const QurasJs = require('quras-js')
var util = require('util');
var syncMysql = require('sync-mysql');

const constants = require('./constants');

var pool = mysql.createPool(constants.MYSQL_OPTIONS);
const rpcServer = new QurasJs.rpc.RPCClient(QurasJs.CONST.QURAS_NETWORK.MAIN);

var syncConnection = new syncMysql(config.database);

function usage () {
	console.log ("usage: node index.js --add <name> [username] [password] [dep dep ...]");
	console.log ("       node index.js --remove <name>");
	console.log ("       node index.js --run");
	process.exit (-1);
}

if (process.argv[2] == "--add" && process.argv.length >= 4) {
	var options = {
		programArgs: ["--run"]
	};

	if (process.argv.length > 4)
		options.username = process.argv[4];

	if (process.argv.length > 5)
		options.password = process.argv[5];
		
	if (process.argv.length > 6)
		options.dependencies = process.argv.splice(6);

	service.add (process.argv[3], options, function(error) {
		if (error)
			console.log(error.toString());
	});
} else if (process.argv[2] == "--remove" && process.argv.length >= 4) {
	service.remove (process.argv[3], function(error) {
		if (error)
			console.log(error.toString());
	});
} else if (process.argv[2] == "--run") {
	service.run (logStream, function () {
		service.stop (0);
	});

	var date = new Date();
	var logStream;
	var Service_Delay;

	logStream = fs.createWriteStream("log" + "_" + date.getFullYear().toString() + "_" + (date.getMonth() + 1).toString() + "_" + date.getDate().toString() + ".log", {'flags': 'a'});

	async.forever(
		function (Do_Work){
			async.waterfall([
						function getConn(callback) {
							pool.getConnection(function(err,conn) {
								var connection = conn;
								if (err)
								{
									callback(err, connection);
								} else {
									//console.log("Connect to Database => Success!");
									//logStream.write(new Date ().toString () + "\t" + "Connect to Database => Success!" + "\n");
									callback(null, connection);
								}
							});
						},
						function checkTask(connection, callback) {
							var sql = 'SELECT * FROM status';

							connection.query(sql, [], function(err, rows) {
								if (err)
								{
									callback(err, connection);
								}
								else
								{
									var count = rows.length;
									if (count == 1)
									{
										var current_blockNum = rows[0]['current_block_number'];
										//logStream.write(new Date ().toString () + "\t" + "Current DB Block Number : " + rows[0]['current_block_number'] + "\n");
										console.log("Current DB Block Number : " + rows[0]['current_block_number']);
										callback(null, connection, current_blockNum);
									}
									else {
										callback(constants.ERR_CONSTANTS.err_db_status, connection);
									} 
								}
							})
						},
						function GetBlockchianBlockNum(connection, current_blockNum, callback)
						{
							rpcServer.getBlockCount()
								.then((block_num) => {
									if (block_num > current_blockNum + 1)
									{
										Service_Delay = constants.FAST_DELAY;
									}
									else{
										Service_Delay = constants.NORMAL_DELAY;
									}
									
									callback(null, connection, current_blockNum);
								})
								.catch ((error) => {
									callback(constants.ERR_CONSTANTS.err_rpc_server, connection, error);
							});
						},
						function GetBlockFromRPCServer(connection, current_blockNum, callback)
						{
							rpcServer.getBlock(current_blockNum)
								.then((block) => {
									callback(null, connection, block, current_blockNum);
								})
								.catch ((error) => {
									callback(constants.ERR_CONSTANTS.err_no_blockNum, connection, error);
							});
							
						},
						function RegBlockDataOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO blocks (hash, size, version, prev_block_hash, merkle_root, time, block_number, nonce, next_consensus, script, tx_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
							var script = JSON.stringify(block['script']);
							
							connection.query(sql, [block['hash'], block['size'], block['version'], block['previousblockhash'], block['merkleroot'], block['time'], block['index'], block['nonce'], block['nextconsensus'], script, block['tx'].length], function(err, rows) {
								if (err)
								{
									callback(err, connection);
								} else {
									callback(null, connection, block, current_blockNum);
								}
							});
						},
						function RegTransactionDataOnDB(connection, block, current_blockNum, callback)
						{
							var transactions =[];
							var sql = 'REPLACE INTO transactions (txid, size, tx_type, version, attribute, vin, vout, sys_fee, net_fee, scripts, nonce, block_number) VALUES ?';

							try {
								var blockNum = block['index'];
								var txs = block['tx'];
								
								for (var i = 0; i < txs.length; i ++)
								{
									var transaction = [txs[i]['txid'], 
													txs[i]['size'], 
													txs[i]['type'],
													txs[i]['version'], 
													JSON.stringify(txs[i]['attributes']), 
													JSON.stringify(txs[i]['vin']), 
													JSON.stringify(txs[i]['vout']),
													txs[i]['sys_fee'], 
													txs[i]['net_fee'], 
													JSON.stringify(txs[i]['scripts']), 
													txs[i]['nonce'],
													blockNum];
									transactions.push(transaction);
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							connection.query(sql, [transactions], function(err, rows) {
								if (err)
								{
									callback(err, connection);
								} else {
									callback(null, connection, block, current_blockNum);
								}
							});
						},
						function IssueTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO issue_transaction (txid) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'IssueTransaction')
									{
										var transaction = [
											txs[i]['txid']
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}
							
							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function ClaimTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO claim_transaction (txid, claims) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'ClaimTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											JSON.stringify(txs[i]['claims'])
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}
							
							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function ClaimTransactionToUtxosStatusOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'UPDATE utxos SET claimed = 1 WHERE txid = ? AND tx_out_index = ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'ClaimTransaction')
									{
										for (var j = 0; j < txs[i]['claims'].length; j++) {
											var transaction = [
												txs[i]['claims'][j]['txid'],
												txs[i]['claims'][j]['vout']
											]
										}
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}
							
							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								try {
									for (var i = 0; i < transactions.length; i++) {
										var rows = syncConnection.query(sql, [transactions[i][0], transactions[i][1]]);
									}
									callback(null, connection, block, current_blockNum);
								}
								catch (ex) {
									callback(ex, connection);
									return;
								}
							}
						},
						function EnrollmentTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO enrollment_transaction (txid, pubkey) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'EnrollmentTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]['pubkey']
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function RegisterTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO register_transaction (txid, type, name, amount, _precision, owner, admin) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'RegisterTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]['asset']['type'],
											txs[i]['asset']['name'][0]['name'],
											txs[i]['asset']['amount'],
											txs[i]['asset']['precision'].toString(),
											txs[i]['asset']['owner'],
											txs[i]['asset']['admin'],
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function ContractTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO contract_transaction (txid, _from, _to, asset) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'ContractTransaction')
									{
										var fromAddr = [];
										var toAddr = [];
										var asset = [];

										/*
										for (var j = 0; j < txs[i]['scripts'].length; j ++)
										{
											var scripts = txs[i]['scripts'][j]['verification'];
											var pubKey;
	
											if (scripts.length == 70)
											{
												pubKey = scripts.slice(2, scripts.length - 2);
											}
											else
											{
												if (scripts.slice(scripts.length - 2, scripts.length) == 'ae')
												{
													pubKey = scripts.slice(4, 70);
												}
											}
											var scriptHash = Module.wallet.getScriptHashFromPublicKey(pubKey);
											fromAddr.push(Module.wallet.getAddressFromScriptHash(scriptHash));
										}
										*/
	
										for (var j = 0; j < txs[i]['vout'].length; j++)
										{
											if (!fromAddr.includes(txs[i]['vout'][j]['address']))
											{
												if (!toAddr.includes(txs[i]['vout'][j]['address']))
												{
													toAddr.push(txs[i]['vout'][j]['address']);
												}
											}
	
											if (!asset.includes(txs[i]['vout'][j]['asset']))
											{
												asset.push(txs[i]['vout'][j]['asset']);
											}
										}
	
										if (toAddr.length == 0)
										{
											toAddr.push(fromAddr[0]);
										}
										
										var transaction = [
											txs[i]['txid'],
											fromAddr.toString(),
											toAddr.toString(),
											asset.toString()
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										for (var i = 0; i < txs.length; i ++)
										{
											if (txs[i]['type'] == 'ContractTransaction')
											{
												var vinTxid = [];
												var curTxid = [];
												var index = 0;
												for (var j = 0; j < txs[i]['vin'].length; j++)
												{
													var txid = txs[i]['vin'][j]['txid'];
													var vout = txs[i]['vin'][j]['vout'];
													
													vinTxid.push(txid);
													curTxid.push(txs[i]['txid']);

													var sql = 'SELECT * FROM transactions WHERE txid=?';

													try {
														var rows = syncConnection.query(sql, [txid]);

														var item = JSON.parse(rows[0]['vout']);
														var vin_addr = item[vout]['address'];

														sql = 'UPDATE contract_transaction SET _from = ? WHERE txid = ?';
														rows = syncConnection.query(sql, [vin_addr, curTxid[index]]);
													}
													catch (ex) {
														callback(ex, connection);
														return;
													}
												}
											}
										}
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function AnonymousContractTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO anonymous_contract_transaction (txid, byJoinSplit, joinsplitPubkey, joinsplitSig, _from, _to, asset) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'AnonymousContractTransaction')
									{
										var fromAddr = [];
										var toAddr = [];
										var asset = [];
	
										for (var j = 0; j < txs[i]['vout'].length; j++)
										{
											if (!fromAddr.includes(txs[i]['vout'][j]['address']))
											{
												if (!toAddr.includes(txs[i]['vout'][j]['address']))
												{
													toAddr.push(txs[i]['vout'][j]['address']);
												}
											}
	
											if (!asset.includes(txs[i]['vout'][j]['asset']))
											{
												asset.push(txs[i]['vout'][j]['asset']);
											}
										}
	
										if (toAddr.length == 0)
										{
											toAddr.push('Anonymous Address');
										}
										
										var transaction = [
											txs[i]['txid'],
											txs[i]['byJoinSplit'],
											txs[i]['joinsplitPubkey'],
											txs[i]['joinsplitSig'],
											fromAddr.toString(),
											toAddr.toString(),
											asset.toString()
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										for (var i = 0; i < txs.length; i ++)
										{
											if (txs[i]['type'] == 'AnonymousContractTransaction')
											{
												var vinTxid = [];
												var curTxid = [];
												var index = 0;
												for (var j = 0; j < txs[i]['vin'].length; j++)
												{
													var txid = txs[i]['vin'][j]['txid'];
													var vout = txs[i]['vin'][j]['vout'];
													
													vinTxid.push(txid);
													curTxid.push(txs[i]['txid']);

													var sql = 'SELECT * FROM transactions WHERE txid=?';
													try {
														var rows = syncConnection.query(sql, [txid]);

														if (rows.length > 0)
															{
																var item = JSON.parse(rows[0]['vout']);
																var vin_addr = item[vout]['address'];
	
																sql = 'UPDATE anonymous_contract_transaction SET _from = ? WHERE txid = ?';
																syncConnection.query(sql, [vin_addr, txs[i]['txid']]);
																index ++;
															}
															else
															{
																sql = 'UPDATE anonymous_contract_transaction SET _from = ? WHERE txid = ?';
																syncConnection.query(sql, ['Anonymous Address', txs[i]['txid']]);
																index ++;
															}
													}
													catch (ex) {
														callback(ex, connection);
														return;
													}
												}
											}
										}
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function HostContractTransactionOnDB(connection, block, current_blockNum, callback)
						{
							callback(null, connection, block, current_blockNum);
						},
						function StateTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO state_transaction (txid, descriptors) VALUES ?';
							var transactions =[];
							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'StateTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]['descriptors']
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function PublishTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO publish_transaction (txid, contract_code_hash, contract_code_script, contract_code_parameters, contract_code_returntype, contract_needstorage, contract_name, contract_version, contract_author, contract_email, contract_description) VALUES ?';
							var transactions =[];
							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'PublishTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]["contract"]["code"]["hash"],
											txs[i]["contract"]["code"]["script"],
											txs[i]["contract"]["code"]["parameters"],
											txs[i]["contract"]["code"]["returntype"],
											txs[i]["contract"]["needstorage"],
											txs[i]["contract"]["name"],
											txs[i]["contract"]["version"],
											txs[i]["contract"]["author"],
											txs[i]["contract"]["email"],
											txs[i]["contract"]["description"]
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}
							
							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function InvocationTransactionOnDB(connection, block, current_blockNum, callback)
						{
							var sql = 'REPLACE INTO invocation_transaction (txid, script, gas) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'InvocationTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]['script'],
											txs[i]['gas']
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function RegAssetTransactionOnDB(connection, block, current_blockNum, callback) {
							var sql = 'REPLACE INTO register_transaction (txid, type, name, amount, _precision, owner, admin, issure) VALUES ?';
							var transactions =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'InvocationTransaction')
									{
										var script = txs[i]['script'];
										const sb = new QurasJs.sc.ScriptBuilder(script)
										var params = sb.toScriptParams();
										
										if (params[0].SysCall == "Pure.Asset.Create" || params[0].SysCall == "Quras.Asset.Create") {
											var asset_type = parseInt(params[0].args[0], 16)
											var asset_type_name;

											switch (asset_type) {
												case 0x40:
													asset_type_name = "CreditFlag"
													break
												case 0x80:
													asset_type_name = "DutyFlag"
													break
												case 0x00:
													asset_type_name = "GoverningToken"
													break
												case 0x01:
													asset_type_name = "UtilityToken"
													break
												case 0x08:
													asset_type_name = "Currency"
													break
												case 0x90:
													asset_type_name = "Share"
													break
												case 0x98:
													asset_type_name = "Invoice"
													break
												case 0x60:
													asset_type_name = "Token"
													break
												case 0x61:
													asset_type_name = "AnonymousToken"
													break
												case 0x62:
													asset_type_name = "TransparentToken"
													break
											}

											var asset_name = JSON.parse(QurasJs.u.hexstring2str(params[0].args[1]))[0].name
											var asset_amount = parseInt(QurasJs.u.reverseHex(params[0].args[2]), 16) / 100000000
											var asset_precision = params[0].args[3]
											var asset_owner = QurasJs.u.reverseHex(params[0].args[4])
											var asset_admin = QurasJs.wallet.getAddressFromScriptHash(QurasJs.u.reverseHex(params[0].args[5]))
											var asset_issure = QurasJs.wallet.getAddressFromScriptHash(QurasJs.u.reverseHex(params[0].args[6]))

											var transaction = [
												txs[i].txid,
												asset_type_name,
												asset_name,
												asset_amount,
												asset_precision,
												asset_owner,
												asset_admin,
												asset_issure
											]

											transactions.push(transaction)
										}
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function MinerTransactionOnDB(connection, block, current_blockNum, callback){
							var sql = 'REPLACE INTO miner_transaction (txid, nonce) VALUES ?';
							var transactions =[];

							try {
								var blockNum = block['index'];
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									if (txs[i]['type'] == 'MinerTransaction')
									{
										var transaction = [
											txs[i]['txid'],
											txs[i]['nonce']
										];
										transactions.push(transaction);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							if (transactions.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
							else 
							{
								connection.query(sql, [transactions], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
						},
						function AddUtxosOnDB(connection, block, current_blockNum, callback){
							var sql = 'REPLACE INTO utxos (txid, tx_out_index, asset, value, address, status) VALUES ?';
							var utxos =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									for (var j = 0; j < txs[i]['vout'].length; j++)
									{
										var utxo = [txs[i]['txid'], 
													txs[i]['vout'][j]['n'], 
													txs[i]['vout'][j]['asset'],
													txs[i]['vout'][j]['value'],
													txs[i]['vout'][j]['address'],
													'unspent'];
										utxos.push(utxo);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}
							
							if (utxos.length > 0)
							{
								connection.query(sql, [utxos], function(err, rows) {
									if (err)
									{
										callback(err, connection);
									} else {
										callback(null, connection, block, current_blockNum);
									}
								});
							}
							else
							{
								callback(null, connection, block, current_blockNum);
							}
						},
						function UpdateUtxosStatusOnDB(connection, block, current_blockNum, callback){
							var sql = 'UPDATE utxos SET status = ? WHERE txid = ? AND tx_out_index = ?';
							var utxos =[];

							try {
								var txs = block['tx'];
								for (var i = 0; i < txs.length; i ++)
								{
									for (var j = 0; j < txs[i]['vin'].length; j++)
									{
										var utxo = ['spent',
													txs[i]['vin'][j]['txid'], 
													txs[i]['vin'][j]['vout']];
	
										utxos.push(utxo);
									}
								}
							}
							catch (ex)
							{
								callback(ex, connection);
								return;
							}

							var errs;
							var result_index = 0;
							for (var i = 0; i < utxos.length; i ++)
							{
								connection.query(sql, [utxos[i][0], utxos[i][1], utxos[i][2]], function(err, rows) {
									result_index ++;
									if (err)
									{
										errs += err;
									}

									if (result_index == utxos.length)
									{
										if (errs)
										{
											callback(errs, connection);
										}
										else{
											callback(null, connection, block, current_blockNum);
										}
									}
								});
							}

							if (utxos.length == 0)
							{
								callback(null, connection, block, current_blockNum);
							}
						},
						function UpdateTaskID(connection, block, current_blockNum, callback) {
							var sql = 'UPDATE status SET current_block_number = ?, last_block_time = ?, block_version = ?';
							connection.query(sql, [current_blockNum + 1, block['time'], block['version']], function(err, rows) {
								if (err)
								{
									callback(err, connection);
								}
								else
								{
									callback(null, connection);
								}
							});
						}
					], function (err, connection)
					{
						if (connection)
						{
							connection.release();
						}

						if (err)
						{
							if (err == constants.ERR_CONSTANTS.success)
							{
								logStream.write(new Date ().toString () + "\t" + "Successfully Finished" + "\n");
								console.log("Successfully Finished");
								service.stop(0);
							}
							else if (err == constants.ERR_CONSTANTS.err_no_blockNum)
							{
								setTimeout(function() {
									Do_Work();
								}, Service_Delay);
							}
							else {
								logStream.write(new Date ().toString () + "\t" + err + "\n");
								console.log(err);

								if (err == "Error! TaskId Was not registered!")
								{
									service.stop(0);
								}
								else if (err == "Error! There are several taskIds")
								{
									service.stop(0);
								}
								else
								{
									setTimeout(function() {
										Do_Work();
									}, constants.FAST_DELAY);
								}
							}
						}
						else {
							var duration = common.cryptoRandomNumber(30, 60);
							setTimeout(function() {
								Do_Work();
							}, Service_Delay);
						}
					}
					);
		},
		function (error){
			logStream.write(new Date ().toString () + "\t" + error + "\n");
			console.log(error);
		}
	);
} else {
	usage ();
}