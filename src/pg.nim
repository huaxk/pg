import db_postgres
import json, strutils, strformat
import sugar, typetraits
import parsecfg

# let cfg{.global.} = loadConfig("config.ini")
# getSectionValue("DB", "ConnectionString")
const cfg* = "host=localhost dbname=gisdb user=gis password=gis"
var conn: DbConn

proc dbOpen*(): DbConn{.discardable.} =
  if conn == nil:
    conn = open("", "", "", cfg)
  return conn

proc dbClose*() =
  if conn != nil:
    conn.close()
    conn = nil

proc getAll*[T](sqlstr: string, args: varargs[string, `$`]): seq[T] =
  result = default(seq[T])
  let sqlfmt = fmt"select to_json(t) from ({sqlstr}) t"
  for r in dbOpen().fastRows(sql sqlfmt, args):
    result.add(parseJson(r[0]).to(T))

proc getOne*[T](sqlstr: string, args: varargs[string, `$`]): T =
  result = default(T)
  let sqlfmt = fmt"select to_json(t) from ({sqlstr}) t"
  let one = dbOpen().getRow(sql sqlfmt, args)
  if one.len > 0:
    result = parseJson(one[0]).to(T)

proc getValue*[T](sqlstr: string, args: varargs[string, `$`]): T =
  result = default(T)
  result = cast[T](dbOpen().getValue(sql sqlstr, args))

proc save*[T](obj: T): bool{.discardable.} =
  var (names, values) = ("", "")
  for name, value in obj.fieldPairs:
    names &= name & ","
    values &= "'" & $value & "',"
  names.removeSuffix(',')
  values.removeSuffix(',')
  # let sqlfmt = fmt"insert into {toLower($T)} ({names}) values ({vaules})"
  let sqlfmt = "insert into " & toLower($T) & "s (" & names & ") values (" & values & ")"
  echo sqlfmt
  result = dbOpen().tryExec(sql sqlfmt)

proc update*(sqlstr: string, args: varargs[string, `$`]): int64{.discardable.} =
  result = dbOpen().execAffectedRows(sql sqlstr, args)

proc delete*(sqlstr: string, args: varargs[string, `$`]): bool{.discardable.} =
  result = dbOpen().tryExec(sql sqlstr, args)

proc exec*(sqlstr: string, args: varargs[string, `$`]): bool{.discardable.} =
  result = dbOpen().tryExec(sql sqlstr, args)

proc begin*() =
  dbOpen().exec(sql"begin")

proc commit*() =
  dbOpen().exec(sql"commit")

proc rollback*() =
  dbOpen().exec(sql"rollback")