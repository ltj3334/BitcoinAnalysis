import csv
import sqlite3
import time
import datetime

STIME = time.time()

conn = sqlite3.connect('./dbv3-service.db')
cur = conn.cursor()
cur.execute('''ATTACH DATABASE './dbv3-index.db' AS DBINDEX;''')
cur.execute('''ATTACH DATABASE './dbv3-core.db' AS DBCORE;''')
cur.execute('''ATTACH DATABASE './dbv3-util.db' AS DBUTIL;''')
conn.commit()

cur.execute('''PRAGMA journal_mode = NORMAL;''')
cur.execute('''PRAGMA synchronous = WAL;''')
cur.execute('''DROP TABLE IF EXISTS Feature;''')
cur.execute('''CREATE TABLE IF NOT EXISTS Feature (
                 addr INTEGER PRIMARY KEY,
                 updatetime INTEGER NOT NULL,
                 cnttx INTEGER NOT NULL,
                 cnttxin INTEGER NOT NULL,
                 cnttxout INTEGER NOT NULL,
                 btc REAL NOT NULL,
                 btcin REAL NOT NULL,
                 btcout REAL NOT NULL,
                 cntuse INTEGER NOT NULL,
                 cntusein INTEGER NOT NULL,
                 cntuseout INTEGER NOT NULL,
                 age INTEGER NOT NULL,
                 agein INTEGER NOT NULL,
                 ageout INTEGER NOT NULL,
                 addrtypep2pkh INTEGER NOT NULL,
                 addrtypep2sh INTEGER NOT NULL,
                 addrtypebech32 INTEGER NOT NULL,
                 addrtypeother INTEGER NOT NULL
               );''')
conn.commit()


def get_feature(conn, cur, addr):
    result = dict()
    result['addr'] = addr
    result['updatetime'] = int(datetime.datetime.now().timestamp())
    # tx
    cur.execute('''SELECT COUNT(tx)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?);''', (addr, addr))
    result['cnttx'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DISTINCT DBCORE.TxIn.tx)
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cnttxin'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DISTINCT DBCORE.TxOut.tx)
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cnttxout'] = cur.fetchone()[0] # Always return
    # btc
    cur.execute('''SELECT A.btc + B.btc
                   FROM (
                     SELECT SUM(DBCORE.TxOut.btc) AS btc
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.Addr = ?) AS A
                     , (
                     SELECT SUM(DBCORE.TxOut.btc) AS btc
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.Addr = ?) AS B;''', (addr, addr))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btc'] = res
    cur.execute('''SELECT SUM(DBCORE.TxOut.btc) AS btc
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.Addr = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btcin'] = res
    cur.execute('''SELECT SUM(DBCORE.TxOut.btc) AS btc
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.Addr = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['btcout'] = res
    # use
    cur.execute('''SELECT COUNT(tx)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION ALL
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?);''', (addr, addr))
    result['cntuse'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DBCORE.TxIn.tx)
                   FROM DBCORE.TxIn
                   INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                          AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cntusein'] = cur.fetchone()[0] # Always return
    cur.execute('''SELECT COUNT(DBCORE.TxOut.tx)
                   FROM DBCORE.TxOut
                   WHERE DBCORE.TxOut.addr = ?;''', (addr, ))
    result['cntuseout'] = cur.fetchone()[0] # Always return
    # age
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?
                     UNION
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr, addr))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['age'] = res
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxIn.tx AS tx
                     FROM DBCORE.TxIn
                     INNER JOIN DBCORE.TxOut ON DBCORE.TxIn.ptx = DBCORE.TxOut.tx
                                            AND DBCORE.TxIn.pn = DBCORE.TxOut.n
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['agein'] = res
    cur.execute('''SELECT MAX(DBCORE.BlkTime.unixtime) - MIN(DBCORE.BlkTime.unixtime)
                   FROM (
                     SELECT DBCORE.TxOut.tx AS tx
                     FROM DBCORE.TxOut
                     WHERE DBCORE.TxOut.addr = ?) AS T
                   INNER JOIN DBCORE.BlkTx ON T.tx = DBCORE.BlkTx.tx
                   INNER JOIN DBCORE.BlkTime ON DBCORE.BlkTx.blk = DBCORE.BlkTime.blk''', (addr,))
    res = cur.fetchone()
    if res is None:
        res = 0
    else:
        res = res[0]
    result['ageout'] = res
    # addrtype
    result['addrtypep2pkh'] = 0 # 1
    result['addrtypep2sh'] = 0 # 3
    result['addrtypebech32'] = 0 #bc1
    result['addrtypeother'] = 0
    cur.execute('''SELECT DBINDEX.AddrID.addr
                   FROM DBINDEX.AddrID
                   WHERE DBINDEX.AddrID.id = ?''', (addr,))
    res = cur.fetchone()
    if res is None:
        pass
    elif res[0].startswith('1'):
        result['addrtypep2pkh'] = 1
    elif res[0].startswith('3'):
        result['addrtypep2sh'] = 1
    elif res[0].startswith('bc1'):
        result['addrtypebech32'] = 1
    else:
        result['addrtypeother'] = 1

    return result

cur.execute('''SELECT COUNT(*) FROM AddrList;''')
total = cur.fetchone()[0]

cur.execute('''SELECT addr FROM AddrList;''')
rows = cur.fetchall()


count = 0
cur.execute('BEGIN TRANSACTION')
for row in rows:
    addr = row[0]
    result = get_feature(conn, cur, addr)
    cur.execute('''INSERT OR IGNORE INTO Feature (
                     addr, updatetime,
                     cnttx, cnttxin, cnttxout,
                     btc, btcin, btcout,
                     cntuse, cntusein, cntuseout,
                     age, agein, ageout,
                     addrtypep2pkh, addrtypep2sh, addrtypebech32, addrtypeother) VALUES (
                     ?, ?,
                     ?, ?, ?,
                     ?, ?, ?,
                     ?, ?, ?,
                     ?, ?, ?,
                     ?, ?, ?, ?);''', (result['addr'], result['updatetime'],
                                       result['cnttx'], result['cnttxin'], result['cnttxout'],
                                       result['btc'], result['btcin'], result['btcout'],
                                       result['cntuse'], result['cntusein'], result['cntuseout'],
                                       result['age'], result['agein'], result['ageout'],
                                       result['addrtypep2pkh'], result['addrtypep2sh'], result['addrtypebech32'], result['addrtypeother']))
    count = count + 1
    print(f'[{int(time.time()-STIME)}] {count} / {total}', end='\r')
cur.execute('COMMIT TRANSACTION')
conn.commit()
print(f'[{int(time.time()-STIME)}] {count} / {total} Done')
