##########
#
# This virtual schema adapter locally exposes one or more schemas
# from a remote or local Exasol instance.
#
# If there is more than one schema specified in the properties then the
# resultset of the virtual schema is the UNION-ALL of the query run in each schema.
# This is handy for multi-tenant setups with multiple identical schemas.
# In that scenario you can create a virtual schema that is effectively
# a union of all those tenant schemas.
#
# This script is not meant to be accessed directly. It is accessed
# via the virtual schema framework.
#
# Usage:
#   --/
#   CREATE OR REPLACE PYTHON ADAPTER SCRIPT py_vs_adapter AS
#      <The contents of this file>
#   /;
#
#   DROP FORCE VIRTUAL SCHEMA IF EXISTS test_virtual CASCADE;
#   CREATE VIRTUAL SCHEMA test_virtual
#     USING py_vs_adapter
#     WITH CONNECTION_NAME = 'EXASOL_CONN'
#          SCHEMAS = 'TNT_ONE,TNT_TWO,TNT_THREE'
#          IMPORT_SCRIPT = 'CUSTOM_IMPORTER_SCRIPT'
#          IS_LOCAL = 'FALSE'
#          TENANT_PATTERN  = 'TNT_(.+)' -- causes CLIENT column to contain ONE, TWO, THREE
#          TENANT_COLUMN   = 'CLIENT'
#          TENANT_USERS    = 'TNT_(.+)_USER'
#
# CONNECTION_NAME and SCHEMAS are mandatory.
#
# CONNECTION_NAME should be a valid connection to an Exasol instance
# containing the listed schema(s)
#
# If IMPORT_SCRIPT is specified then, for remote connections,
# that script will be used for importing data (via IMPORT FROM SCRIPT)
# instead of the default 'IMPORT FROM EXA' statement.
# This can allow for faster more optimized calls. In particular this
# allows you to issue a commit after each query by adding that to your
# import script. 'IMPORT FROM EXA' does not issue commits so any remote
# auto-generated indexes are not saved.
#
# If IS_LOCAL is specified then IMPORT_SCRIPT is ignored and the
# query is run locally.
#
# If SCHEMAS lists more than one comma delimited schema then the
# result is the union of running the query in each of those schemas.
# In this case a column will be automatically added to each table
# indicating what schema this result row came from. The name of this
# column can be specified by the optional TENANT_COLUMN property.
# If you want a subset of the schema name to be considered as the
# tenant's name in the result set then you can specify and optional
# TENANT_PATTERN property which should be a regexp containing 1 capture group.
#
# If TENANT_USERS is defined and the current username matches it then
# results will only be returned for the current user's schema regardless
# of TENANT_COLUMN criteria. This only applies if there are multiple SCHEMAS.
#
##########

import re
import copy
import cjson

###
# This is the main entry point used by the virtual schema API
###
def adapter_call(requestJS):
    req = cjson.decode(requestJS)
    reqType = req['type']
    res = { 'type' : reqType }

    if reqType in [ 'createVirtualSchema','refresh','setProperties' ] :
        res['schemaMetadata'] = getSchemaMetadata(req)
    elif reqType == 'dropVirtualSchema': pass
    elif reqType == 'getCapabilities': res['capabilities'] = getCapabilities()
    elif reqType == 'pushdown': res['sql'] = getPushdownSQL(req)
    else: raise ValueError('Unsupported callback: ' + reqType)

    # Exasol expects UTF-8 encoded strings of type str
    # in the return JSON. Unicode is not yet supported
    return cjson.encode(res).encode('utf-8')


def getProperties(req):
    if not 'properties' in req['schemaMetadataInfo']:
        raise ValueError('Config error: required properties: ' +
                         "'SCHEMAS' and 'CONNECTION_NAME' not given")
    if not 'SCHEMAS' in req['schemaMetadataInfo']['properties']:
        raise ValueError("Config error: required property 'SCHEMAS' not given")
    if not 'CONNECTION_NAME' in req['schemaMetadataInfo']['properties']:
        raise ValueError('Config error: required property ' +
                         "'CONNECTION_NAME' not given")
    props = {}
    props['connection']   = req['schemaMetadataInfo']['properties']['CONNECTION_NAME']
    props['schemas']      = req['schemaMetadataInfo']['properties']['SCHEMAS'].upper().split(',')
    props['isLocal']      = req['schemaMetadataInfo']['properties'].pop('IS_LOCAL','FALSE')
    props['importScript'] = req['schemaMetadataInfo']['properties'].pop('IMPORT_SCRIPT',  None)
    props['tenantPattern']= req['schemaMetadataInfo']['properties'].pop('TENANT_PATTERN', '(.+)')
    props['tenantColumn'] = req['schemaMetadataInfo']['properties'].pop('TENANT_COLUMN',  'TENANT')
    props['tenantUsers']  = req['schemaMetadataInfo']['properties'].pop('TENANT_USERS',  None)
    return props


def getSchemaMetadata(req):
    import sys
    import glob
    sys.path.extend(glob.glob('/buckets/bfsdefault/default/ws/*'))
    import EXASOL

    props = getProperties(req)
    connection_name = props['connection']
    tenantColumn = props['tenantColumn']
    schemas = props['schemas']
    tables = []
    if 'requestedTables' in req:
        tables = res['requestedTables'] = req['requestedTables']

    # exa.get_connection is a global function available to all python scripts
    c = exa.get_connection(connection_name)
    cursor = EXASOL.connect('ws://'+c.address, c.user, c.password).cursor()

    # First validate any views that may have never been validated.
    # Doing so populates exa_all_columns with their info.
    cursor.execute('''
       SELECT view_name
       FROM exa_all_views
       WHERE view_schema = '%s'
       ''' % (schemas[0] )
    )
    if cursor.rowcount > 0:
        views = []
        for row in cursor:
            views.append(row[0])
        for view in views:
            # The view might be invalid so we just try and ignore if it fails
            try:
                cursor.execute('DESC "%s"."%s"' % (schemas[0], view))
            except:
                pass

    cursor.execute('''
        SELECT column_table, column_name,
               column_type, column_maxsize,
               column_num_prec, column_num_scale
        FROM exa_all_columns
        WHERE column_schema='%s'
        ORDER BY column_table, column_ordinal_position
         ''' % ( schemas[0] )
    )
    res = []
    prevTable = ''
    if cursor.rowcount > 0:
        for row in cursor:
            table, column, type, maxsize, prec, scale = row
            if len(tables) > 0 and table not in tables:
                continue

            if prevTable != table:
                prevTable = table
                cols = []
                res.append({
                    'name':    table,
                    'columns': cols
                })
                # If we are unioning multiple schemas each table gets a tenant column
                if len(schemas) > 1:
                    cols.append({
                        'name': tenantColumn,
                        'dataType': {
                            'type': 'VARCHAR',
                            'size': 50
                         }
                    })

            if len(schemas) > 1 and column == tenantColumn:
                continue

            cols.append({
                'name':     column,
                'dataType': getDataTypeDef(type, maxsize, prec, scale)
            })

    return { 'tables': res }


###
# This is the list of query "parts" that will be pushed down to the remote Exasol instance.
# These are gleaned from and should be periodically be kept up-to-date with
# https://github.com/exasol/virtual-schema-common-java/tree/master/src/main/java/com/exasol/adapter/capabilities
# We want to support pretty much ALL capabilities because we are just connecting to
# Exasol on the backend so it can support anything the frontend can support.
###
def getCapabilities():
    capabilities = [
        'SELECTLIST_PROJECTION', 'SELECTLIST_EXPRESSIONS', 'FILTER_EXPRESSIONS',
        'AGGREGATE_SINGLE_GROUP', 'AGGREGATE_GROUP_BY_COLUMN',
        'AGGREGATE_GROUP_BY_EXPRESSION', 'AGGREGATE_GROUP_BY_TUPLE', 'AGGREGATE_HAVING',
        'ORDER_BY_COLUMN', 'ORDER_BY_EXPRESSION',
        'LIMIT', 'LIMIT_WITH_OFFSET',
        'JOIN', 'JOIN_TYPE_INNER', 'JOIN_TYPE_LEFT_OUTER', 'JOIN_TYPE_RIGHT_OUTER',
        'JOIN_TYPE_FULL_OUTER', 'JOIN_CONDITION_EQUI', 'JOIN_CONDITION_ALL'
    ]

    literalCapabilities = [
        'NULL', 'BOOL', 'DATE',
        'TIMESTAMP', 'TIMESTAMP_UTC',
        'DOUBLE', 'EXACTNUMERIC',
        'STRING', 'INTERVAL'
    ]

    predicateCapabilities = [
        'AND', 'OR', 'NOT', 'EQUAL', 'NOTEQUAL', 'LESS', 'LESSEQUAL', 'LIKE',
        'LIKE_ESCAPE', 'REGEXP_LIKE', 'BETWEEN', 'IN_CONSTLIST',
        'IS_NULL', 'IS_NOT_NULL', 'IS_JSON', 'IS_NOT_JSON'
    ]


    scalarFunctionCapabilities = [
        'ADD','SUB','MULT','FLOAT_DIV','NEG','ABS','ACOS','ASIN','ATAN','ATAN2','CEIL','COS',
        'COSH','COT','DEGREES','DIV','EXP','FLOOR','GREATEST','LEAST','LN','LOG','MOD',
        'POWER','RADIANS','RAND','ROUND','SIGN','SIN','SINH','SQRT','TAN','TANH','TRUNC',
        'ASCII','BIT_LENGTH','CHR','COLOGNE_PHONETIC','CONCAT','DUMP','EDIT_DISTANCE',
        'INSERT','INSTR','LENGTH','LOCATE','LOWER','LPAD','LTRIM','OCTET_LENGTH',
        'REGEXP_INSTR','REGEXP_REPLACE','REGEXP_SUBSTR','REPEAT','REPLACE','REVERSE',
        'RIGHT','RPAD','RTRIM','SOUNDEX','SPACE','SUBSTR','TRANSLATE','TRIM','UNICODE',
        'UNICODECHR','UPPER','ADD_DAYS','ADD_HOURS','ADD_MINUTES','ADD_MONTHS','ADD_SECONDS',
        'ADD_WEEKS','ADD_YEARS','CONVERT_TZ','CURRENT_DATE','CURRENT_TIMESTAMP','DATE_TRUNC',
        'DAY','DAYS_BETWEEN','DBTIMEZONE','EXTRACT','HOURS_BETWEEN','LOCALTIMESTAMP',
        'MINUTE','MINUTES_BETWEEN','MONTH','MONTHS_BETWEEN','NUMTODSINTERVAL',
        'NUMTOYMINTERVAL','POSIX_TIME','SECOND','SECONDS_BETWEEN','SESSIONTIMEZONE',
        'SYSDATE','SYSTIMESTAMP','WEEK','YEAR','YEARS_BETWEEN','ST_X','ST_Y','ST_ENDPOINT',
        'ST_ISCLOSED','ST_ISRING','ST_LENGTH','ST_NUMPOINTS','ST_POINTN','ST_STARTPOINT',
        'ST_AREA','ST_EXTERIORRING','ST_INTERIORRINGN','ST_NUMINTERIORRINGS','ST_GEOMETRYN',
        'ST_NUMGEOMETRIES','ST_BOUNDARY','ST_BUFFER','ST_CENTROID','ST_CONTAINS',
        'ST_CONVEXHULL','ST_CROSSES','ST_DIFFERENCE','ST_DIMENSION','ST_DISJOINT',
        'ST_DISTANCE','ST_ENVELOPE','ST_EQUALS','ST_FORCE2D','ST_GEOMETRYTYPE',
        'ST_INTERSECTION','ST_INTERSECTS','ST_ISEMPTY','ST_ISSIMPLE','ST_OVERLAPS',
        'ST_SETSRID','ST_SYMDIFFERENCE','ST_TOUCHES','ST_TRANSFORM','ST_UNION','ST_WITHIN',
        'CAST','IS_NUMBER','IS_BOOLEAN','IS_DATE','IS_DSINTERVAL','IS_YMINTERVAL',
        'IS_TIMESTAMP','TO_CHAR','TO_DATE','TO_DSINTERVAL','TO_YMINTERVAL','TO_NUMBER',
        'TO_TIMESTAMP','BIT_AND','BIT_CHECK','BIT_NOT','BIT_OR','BIT_SET','BIT_TO_NUM',
        'BIT_XOR','CASE','HASH_MD5','HASH_SHA','HASH_SHA1','HASH_SHA256','HASH_SHA512',
        'HASH_TIGER','NULLIFZERO','SYS_GUID','ZEROIFNULL',
        'INITCAP','FROM_POSIX_TIME','HOUR',
        'BIT_LROTATE','BIT_LSHIFT','BIT_RROTATE','BIT_RSHIFT',
        'HASHTYPE_MD5','HASHTYPE_SHA1','HASHTYPE_SHA256','HASHTYPE_SHA512','HASHTYPE_TIGER',
        'JSON_VALUE','MIN_SCALE',
    ]
    # We want these to resolve to the local values so don't push them down
    #'CURRENT_SCHEMA','CURRENT_SESSION','CURRENT_STATEMENT','CURRENT_USER','SESSION_PARAMETER'


    aggregateFunctionCapabilities = [
        'COUNT','COUNT_STAR','COUNT_DISTINCT','SUM','SUM_DISTINCT','MIN','MAX','AVG',
        'AVG_DISTINCT','MEDIAN','FIRST_VALUE','LAST_VALUE','STDDEV','STDDEV_DISTINCT',
        'STDDEV_POP','STDDEV_POP_DISTINCT','STDDEV_SAMP','STDDEV_SAMP_DISTINCT','VARIANCE',
        'VARIANCE_DISTINCT','VAR_POP','VAR_POP_DISTINCT','VAR_SAMP','VAR_SAMP_DISTINCT',
        'GROUP_CONCAT','GROUP_CONCAT_DISTINCT','GROUP_CONCAT_SEPARATOR',
        'GROUP_CONCAT_ORDER_BY','GEO_INTERSECTION_AGGREGATE','GEO_UNION_AGGREGATE',
        'APPROXIMATE_COUNT_DISTINCT'
        'COUNT_TUPLE','MUL','MUL_DISTINCT','EVERY','SOME',
        'LISTAGG','LISTAGG_DISTINCT','LISTAGG_SEPARATOR',
        'LISTAGG_ON_OVERFLOW_ERROR','LISTAGG_ON_OVERFLOW_TRUNCATE','LISTAGG_ORDER_BY',
    ]


    for cap in literalCapabilities:
        capabilities.append('LITERAL_' + cap)
    for cap in predicateCapabilities:
        capabilities.append('FN_PRED_' + cap)
    for cap in scalarFunctionCapabilities:
        capabilities.append('FN_' + cap)
    for cap in aggregateFunctionCapabilities:
        capabilities.append('FN_AGG_' + cap)

    return capabilities


def getPushdownSQL(req):
    isSelectStar = 'selectList' not in req['pushdownRequest']
    hasOrderBy   = 'orderBy'        in req['pushdownRequest']
    hasLimit     = 'limit'          in req['pushdownRequest']
    props = getProperties(req)
    tenantPattern = props['tenantPattern']
    tenantColumn  = props['tenantColumn']
    schemas =  props['schemas']
    schemas.sort()
    baseSQL = ' '.join(buildSelectSQL(copy.deepcopy(req['pushdownRequest'])))

    allowedSQLs = []
    allSQLs = []
    for schema in schemas:
        sql = baseSQL.replace('$$SCHEMA$$', schema)

        if len(schemas) == 1:
            allowedSQLs.append(sql)
        else:
            # There are multiple schemas/tenants so first
            # extract the tenant's name from the schema name
            tenant = re.sub(tenantPattern, r"\1", schema)
            if tenant == '': tenant = schema

            # If there's more than one schema listed then there's an
            # extra virtual tenant column in the tables so we need to
            # replace any reference to the tenant-column with a static
            # string containing the tenant's name
            sql = re.sub(
                '"[^"]+"."%s"' % (tenantColumn),
                "'%s'" % (tenant),
                sql
            )

            # The virtual tenant column is always the first column in the table
            # so if it's a 'SELECT *' query we manually add that column to the results
            if isSelectStar:
                sql = re.sub("^SELECT", "SELECT '%s'," % (tenant), sql)

            allSQLs.append(sql)

            # If the current_user is only allowed to see one of the schemas
            # then so we only return that tenant's SQL
            if props['tenantUsers'] != None:
                userTenant = re.match(props['tenantUsers'], exa.meta.current_user)
                if userTenant:
                    if userTenant.group(1) == tenant:
                        allowedSQLs.append(sql)
                        break
                    else:
                        continue

            # If the query has criteria that restricts results to a subset
            # of tenants then we only include those tenant's SQL in the final UNION
            # this is only for performance' sake since the criteria itself will also
            # restrict results.
            criteriaPattern = "(NOT \( )?'%s' (=|!=|IN) ('\w+'|\([^)]+\))" % (tenant)
            tenantCrit = re.findall(criteriaPattern, sql)
            if len(tenantCrit) == 1:
                isNot, operator, counties = tenantCrit[0]
                tenantIsListed = re.search("'%s'" % (tenant), counties)
                positiveMatch = operator in ('=','IN') and not isNot
                negativeMatch = operator == '!=' or isNot
                if (positiveMatch and tenantIsListed) or \
                   (negativeMatch and not tenantIsListed):
                    allowedSQLs.append(sql)
            else:
                allowedSQLs.append(sql)

    # Our tenant criteria may cause no rows to match
    # but we gotta return /some/ SQL
    if len(allowedSQLs) == 0:
        allowedSQLs = [ 'SELECT * FROM (%s) WHERE FALSE' % (allSQLs[0]) ];

    if len(allowedSQLs) > 1 and (hasOrderBy or hasLimit):
        # Unions cannot have orderbys or limits in the
        # directly-unioned SQL so we nest them to trick it.
        for i in range(len(allowedSQLs)):
            allowedSQLs[i] = 'SELECT * FROM (%s)' % (allowedSQLs[i])

    finalSQL = ' UNION ALL '.join(allowedSQLs)

    if props['isLocal'].upper() in ('TRUE','YES','1'):
        return finalSQL

    elif props['importScript'] != None:
        return "SELECT %s('%s','%s') EMITS (%s)" % (
            props['importScript'],
            props['connection'],
            finalSQL.replace("'","''"),
            ','.join(buildEmitColumns(req))
        )

    else:
        return "IMPORT FROM EXA AT %s STATEMENT '%s'" % (
            props['connection'], finalSQL.replace("'","''")
        )


def buildSelectSQL(req):
    sql = ['SELECT']

    if 'selectList' in req:
        selectList = req.pop('selectList')
        if len(selectList) == 0:
            sql.append('TRUE')
        else:
            sql += buildSQLNode(selectList)
    else:
        # This is a case of "SELECT * FROM ..."
        # It only happens if there is a single table in the FROM.
        # We need to preface the * with the table name incase
        # we later need to add a tenant desgination
        table = buildSQLNode(copy.deepcopy(req['from']))[-1]
        sql.append(table+'.*')

    sql.append('FROM')
    sql += buildSQLNode(req.pop('from'))

    if 'filter' in req:
        sql.append('WHERE')
        sql += buildSQLNode(req.pop('filter'))

    if 'aggregationType' in req:
        aggType = req.pop('aggregationType')
        if aggType == 'group_by':
            sql.append('GROUP BY')
            sql += buildSQLNode(req.pop('groupBy'))
        elif aggType == 'single_group':
            pass
        else:
            raise ValueError('Aggregation type '+aggType+' not supported')

    if 'having' in req:
        sql.append('HAVING')
        sql += buildSQLNode(req.pop('having'))

    if 'orderBy' in req:
        sql.append('ORDER BY')
        sql += buildSQLNode(req.pop('orderBy'))

    if 'limit' in req:
        sql.append('LIMIT')
        sql += buildSQLNode(req.pop('limit'))

    req.pop('type')
    req.pop('filter_expr_string_for_debug','')
    if len(req) > 0:
        raise ValueError(
            'Unhandled pushdown parameters: ' +
            cjson.encode(req).encode('utf-8')
        )

    return sql


def buildSQLNode(req, delim=','):
    sql = []

    if type(req) == list:
        for expr in req:
            sql.append(' '.join(buildSQLNode(expr)))
        return [(' '+delim+' ').join(sql)]

    elif type(req) == str:
        return [req]

    Type = req.pop('type','NO_TYPE')

    if Type == 'column':
        table = req.pop('tableName')
        table = req.pop('tableAlias', table)
        column = req.pop('name')
        sql.append('"%s"."%s"' % ( table, column ))
        req.pop('columnNr')

    elif Type == 'function_aggregate':
        sql.append(req.pop('name')+'(')
        if req.pop('distinct',''):
            sql.append('DISTINCT')
        sql += buildSQLNode(req.pop('arguments','*'))
        sql.append(')')

    elif Type == 'function_aggregate_group_concat':
        sql.append(req.pop('name')+'(')
        if req.pop('distinct',''):
            sql.append('DISTINCT')
        sql += buildSQLNode(req.pop('arguments'))
        if 'orderBy' in req:
            sql.append('ORDER BY')
            sql += buildSQLNode(req.pop('orderBy'))
        if 'separator' in req:
            sql.append(" SEPARATOR '%s'" % req.pop('separator'))
        sql.append(')')

    elif Type == 'function_aggregate_listagg':
        sql.append(req.pop('name')+'(')
        if req.pop('distinct',''):
            sql.append('DISTINCT')
        sql += buildSQLNode(req.pop('arguments'))
        if 'separator' in req:
            sql.append(",")
            sql += buildSQLNode(req.pop('separator'))
        if 'overflowBehavior' in req:
             sql.append('ON OVERFLOW')
             overflow = req.pop('overflowBehavior')
             tp = overflow.pop('type')
             sql.append(tp)
             if tp == 'TRUNCATE':
                sql += buildSQLNode(overflow.pop('truncationFiller'))
                sql.append(overflow.pop('truncationType'))
        sql.append(')')
        if 'orderBy' in req:
            sql.append('WITHIN GROUP ( ORDER BY')
            sql += buildSQLNode(req.pop('orderBy'))
            sql.append(')')

    elif Type == 'function_scalar':
        req.pop('numArgs','')
        req.pop('variableInputArgs','')
        if req.pop('infix',''):
            infix = req.pop('name')
            args  = req.pop('arguments')
            operator = {
                'ADD':  '+',
                'SUB':  '-',
                'MULT': '*',
                'FLOAT_DIV': '/'
            }[infix]
            if operator == '':
                raise ValueError('Unknown infix type '+infix)
            if len(args) != 2:
                raise ValueError('Incorrect infix arg num:',args)
            sql.append('(')
            sql += buildSQLNode(args[0])
            sql.append(operator)
            sql += buildSQLNode(args[1])
            sql.append(')')
        else:
            funcName = req.pop('name')
            if funcName == 'NEG':
                funcName = '-'
            sql.append(funcName+'(')
            sql += buildSQLNode(req.pop('arguments'))
            sql.append(')')

    elif Type == 'function_scalar_case':
        sql.append(req.pop('name'))
        if 'basis' in req: sql += buildSQLNode(req.pop('basis'))
        res  = req.pop('results')
        args = req.pop('arguments')
        while 1:
            if len(res) == 0: break
            if len(args) > 0:
                sql.append('WHEN')
                sql += buildSQLNode(args.pop(0))
                sql.append('THEN')
                sql += buildSQLNode(res.pop(0))
            else:
                sql.append('ELSE')
                sql += buildSQLNode(res.pop(0))
        sql.append('END')

    elif Type == 'function_scalar_cast':
        sql.append(req.pop('name')+'(')
        sql += buildSQLNode(req.pop('arguments')[0])
        sql.append('AS')
        sql.append(dataTypeDefToStr(req.pop('dataType')))
        sql.append(')')

    elif Type == 'function_scalar_extract':
        sql.append(req.pop('name')+'(')
        sql.append(req.pop('toExtract'))
        sql.append('FROM')
        sql += buildSQLNode(req.pop('arguments')[0])
        sql.append(')')

    elif Type == 'function_scalar_json_value':
        sql.append(req.pop('name')+'(')
        sql += buildSQLNode(req.pop('arguments'))
        if 'returningDataType' in req:
            sql.append('RETURNING')
            sql.append(dataTypeDefToStr(req.pop('returningDataType')))
        if 'emptyBehavior' in req:
            empty = req.pop('emptyBehavior')
            sql.append(empty.pop('type'))
            sql += buildSQLNode(empty.pop('expression'))
            sql.append('ON EMPTY')
        if 'errorBehavior' in req:
            error = req.pop('errorBehavior')
            sql.append(error.pop('type'))
            sql += buildSQLNode(error.pop('expression'))
            sql.append('ON ERROR')
        sql.append(')')

    elif Type == 'join':
        sql += buildSQLNode(req.pop('left'))
        sql.append(req.pop('join_type').replace('_',' '))
        sql.append('JOIN')
        sql += buildSQLNode(req.pop('right'))
        sql.append('ON')
        sql += buildSQLNode(req.pop('condition'))

    elif Type in ('literal_exactnumeric','literal_bool','literal_double'):
        sql.append(str(req.pop('value')))

    elif Type == 'literal_date':
        sql.append("DATE '%s'" % req.pop('value'))

    elif Type == 'literal_interval':
        sql.append("INTERVAL '"+req.pop('value')+"' ")
        sql.append(dataTypeDefToStr(req.pop('dataType')))

    elif Type == 'literal_null':
        sql.append('NULL')

    elif Type == 'literal_string':
        sql.append("'%s'" % (req.pop('value').replace("'","''")))

    elif Type == 'literal_timestamp':
        sql.append("TIMESTAMP '%s'" % req.pop('value'))

    elif Type == 'literal_timestamputc':
        sql.append("CONVERT_TZ(TIMESTAMP '%s','UTC', SESSIONTIMEZONE)" % req.pop('value'))

    # This is a LIMIT clause...they have no type key for some reason
    elif 'numElements' in req:
        sql.append(str(req.pop('numElements')))
        if 'offset' in req:
            sql.append('OFFSET')
            sql.append(str(req.pop('offset')))

    elif Type == 'order_by_element':
        sql += buildSQLNode(req.pop('expression'))

        if req.pop('isAscending'):
            sql.append('ASC')
        else:
            sql.append('DESC')

        if req.pop('nullsLast'):
            sql.append('NULLS LAST')
        else:
            sql.append('NULLS FIRST')

    elif Type == 'predicate_and':
        sql.append('(')
        sql += buildSQLNode(req.pop('expressions'), delim='AND')
        sql.append(')')

    elif Type == 'predicate_between':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('BETWEEN')
        sql += buildSQLNode(req.pop('left'))
        sql.append('AND')
        sql += buildSQLNode(req.pop('right'))

    elif Type == 'predicate_in_constlist':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('IN (')
        sql += buildSQLNode(req.pop('arguments'))
        sql.append(')')

    elif Type == 'predicate_is_null':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('IS NULL')

    elif Type == 'predicate_is_not_null':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('IS NOT NULL')

    elif Type == 'predicate_is_json':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('IS JSON')
        sql += buildSQLNode(req.pop('typeConstraint'))
        sql += buildSQLNode(req.pop('keyUniquenessConstraint'))

    elif Type == 'predicate_is_not_json':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('IS NOT JSON')
        sql += buildSQLNode(req.pop('typeConstraint'))
        sql += buildSQLNode(req.pop('keyUniquenessConstraint'))

    elif Type == 'predicate_equal':
        sql += buildSQLNode(req.pop('left'))
        sql.append('=')
        sql += buildSQLNode(req.pop('right'))

    elif Type == 'predicate_less':
        sql += buildSQLNode(req.pop('left'))
        sql.append('<')
        sql += buildSQLNode(req.pop('right'))

    elif Type == 'predicate_lessequal':
        sql += buildSQLNode(req.pop('left'))
        sql.append('<=')
        sql += buildSQLNode(req.pop('right'))

    elif Type == 'predicate_like':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('LIKE')
        sql += buildSQLNode(req.pop('pattern'))
        if 'escapeChar' in req:
            sql.append('ESCAPE')
            sql += buildSQLNode(req.pop('escapeChar'))

    elif Type == 'predicate_like_regexp':
        sql += buildSQLNode(req.pop('expression'))
        sql.append('REGEXP_LIKE')
        sql += buildSQLNode(req.pop('pattern'))

    elif Type == 'predicate_not':
        sql.append('NOT (')
        sql += buildSQLNode(req.pop('expression'))
        sql.append(')')

    elif Type == 'predicate_notequal':
        sql += buildSQLNode(req.pop('left'))
        sql.append('!=')
        sql += buildSQLNode(req.pop('right'))

    elif Type == 'predicate_or':
        sql.append('(')
        sql += buildSQLNode(req.pop('expressions'), delim='OR')
        sql.append(')')

    elif Type == 'table':
        sql.append('"$$SCHEMA$$"."%s"' % ( req.pop('name') ))
        if 'alias' in req:
            sql.append(' AS ')
            sql.append('"%s"' % (req.pop('alias')))

    else:
        raise ValueError(
            'Pushdown request type ' + Type +
            ' not supported: ' + cjson.encode(req).encode('utf-8')
        )

    if len(req) > 0:
        raise ValueError(
            'Unhandled pushdown parameters: ' +
            cjson.encode(req).encode('utf-8')
        )

    return sql


def buildEmitColumns(req):
    tables = req['involvedTables']
    select = req['pushdownRequest']
    colTypes = []
    if 'selectList' in select:
        if len(select['selectList']) == 0:
            colTypes = ['BOOLEAN']
        else:
            for col in select['selectList']:
                colTypes.append(getColumnDataType(col, tables).toStr())
    else:
        # "select *" situation
        table = getTableDef(select['from']['name'], tables)
        for col in table['columns']:
            colTypes.append(dataTypeDefToStr(col['dataType']))

    for i in range(len(colTypes)):
        colTypes[i] = 'C%i %s' % (i, colTypes[i])

    return colTypes


# Returns a DataType Object
def getColumnDataType(req, tables):

    if type(req) == list:
        return getColumnTypeList(req, tables)

    Type = req.get('type','NO_TYPE')
    INT      = DataType('DECIMAL',18)
    DOUBLE   = DataType('DOUBLE')
    BOOLEAN  = DataType('BOOLEAN')
    CLOB     = DataType('CLOB')
    DATE     = DataType('CLOB')
    TIMESTAMP= DataType('CLOB')

    if Type == 'column':
        table = getTableDef(req['tableName'], tables)
        dt = dataTypeFromDef(table['columns'][req['columnNr']]['dataType'])

    elif Type.startswith('function_aggregate'):
        funcName = req['name'].upper()
        if funcName in (
            'COUNT','COUNT_STAR','COUNT_DISTINCT','COUNT_TUPLE','APPROXIMATE_COUNT_DISTINCT'
        ):
            dt = INT

        elif funcName in ('SUM','SUM_DISTINCT','MIN','MAX','MEDIAN'):
            dt = getColumnDataType(req['arguments'], tables)
            if dt.type not in ('DOUBLE','DECIMAL'):
                dt = INT

        elif funcName in (
            'AVG', 'AVG_DISTINCT','STDDEV','STDDEV_DISTINCT','STDDEV_POP',
            'STDDEV_POP_DISTINCT','STDDEV_SAMP','STDDEV_SAMP_DISTINCT','VARIANCE',
            'VARIANCE_DISTINCT','VAR_POP','VAR_POP_DISTINCT','VAR_SAMP','VAR_SAMP_DISTINCT',
            'MUL','MUL_DISTINCT',
        ):
            dt = DOUBLE

        elif funcName in (
            'GROUP_CONCAT','GROUP_CONCAT_DISTINCT','GROUP_CONCAT_SEPARATOR',
            'GROUP_CONCAT_ORDER_BY','GEO_INTERSECTION_AGGREGATE','GEO_UNION_AGGREGATE',
            'LISTAGG','LISTAGG_DISTINCT','LISTAGG_SEPARATOR',
            'LISTAGG_ON_OVERFLOW_ERROR','LISTAGG_ON_OVERFLOW_TRUNCATE','LISTAGG_ORDER_BY',
        ):
            dt = CLOB

        elif funcName in ( 'EVERY','SOME' ):
            dt = BOOLEAN

        elif funcName in ('FIRST_VALUE','LAST_VALUE'):
            dt = getColumnDataType(req['arguments'], tables)

        else:
            raise ValueError('Unknown aggregate function type '+funcName)

    elif Type in ('function_scalar','function_scalar_case','function_scalar_extract'):
        funcName = req['name'].upper()

        # These return INTs
        if funcName in (
            'ABS', 'ACOS', 'ADD', 'ASCII' 'ASIN', 'ATAN', 'ATAN2', 'BIT_AND', 'BIT_CHECK',
            'BIT_LENGTH', 'BIT_NOT', 'BIT_OR', 'BIT_SET', 'BIT_TO_NUM', 'BIT_XOR', 'CEIL',
            'COS', 'COSH', 'COT', 'DAY', 'DAYS_BETWEEN', 'DEGREES', 'DIV', 'EDIT_DISTANCE',
            'EXP', 'FLOAT_DIV', 'FLOOR', 'GREATEST', 'HOURS_BETWEEN', 'INSTR', 'LEAST',
            'LENGTH', 'LN', 'LOCATE', 'LOG', 'MINUTE', 'MINUTES_BETWEEN', 'MOD', 'MONTH',
            'MONTHS_BETWEEN', 'MULT', 'NEG', 'NULLIFZERO', 'OCTET_LENGTH', 'POSIX_TIME',
            'POWER', 'RADIANS', 'RAND', 'REGEXP_INSTR', 'ROUND', 'SECOND', 'SECONDS_BETWEEN',
            'SIGN', 'SIN', 'SINH', 'SQRT', 'SUB', 'TAN', 'TANH', 'TO_NUMBER', 'UNICODE',
            'WEEK', 'YEAR', 'YEARS_BETWEEN', 'ZEROIFNULL', 'HOUR'
        ):
            dt = INT

        # These return smallish strings
        elif funcName in (
            'CHR', 'DBTIMEZONE', 'EXTRACT', 'SESSIONTIMEZONE', 'TO_CHAR', 'UNICODECHR'
        ):
            dt = DataType('VARCHAR',100)

        elif funcName in (
            'BIT_LROTATE','BIT_LSHIFT','BIT_RROTATE','BIT_RSHIFT'
        ):
            dt = DataType('DECIMAL',20)

        # These return large strings
        elif funcName in (
            'COLOGNE_PHONETIC', 'CONCAT', 'DUMP', 'HASH_MD5', 'HASH_SHA', 'HASH_SHA1',
            'HASH_SHA256', 'HASH_SHA512', 'HASH_TIGER', 'INSERT', 'LPAD', 'REGEXP_REPLACE',
            'REGEXP_SUBSTR','REPEAT','REPLACE','RIGHT','RPAD','SOUNDEX','SPACE','SYS_GUID',
            'HASHTYPE_MD5','HASHTYPE_SHA1','HASHTYPE_SHA256','HASHTYPE_SHA512','HASHTYPE_TIGER'
            'JSON_VALUE', 'INITCAP'
        ):
            dt = CLOB

        # Size/Type depends on args
        elif funcName in (
            'ADD_DAYS', 'ADD_HOURS', 'ADD_MINUTES', 'ADD_MONTHS', 'ADD_SECONDS', 'ADD_WEEKS',
            'ADD_YEARS', 'CASE', 'LOWER', 'LTRIM', 'REVERSE', 'RTRIM', 'SUBSTR', 'TRANSLATE',
            'TRIM', 'TRUNC', 'UPPER'
        ):
            dt = getColumnDataType(req['arguments'], tables)

        # Intervals just returned strings for now: TODO handle better
        elif funcName in (
            'NUMTODSINTERVAL', 'NUMTOYMINTERVAL', 'TO_DSINTERVAL','TO_YMINTERVAL'
        ):
            dt = DataType('VARCHAR',100)

        # These return TIMESTAMPs
        elif funcName in (
            'CONVERT_TZ','CURRENT_TIMESTAMP','LOCALTIMESTAMP','SYSTIMESTAMP','TO_TIMESTAMP',
            'FROM_POSIX_TIME',
        ):
            dt = TIMESTAMP

        # These return DATEs
        elif funcName in (
            'CURRENT_DATE', 'DATE_TRUNC', 'SYSDATE', 'TO_DATE'
        ):
            dt = DATE

        # Geos just returned as strings for now: TODO handle better
        elif funcName in (
            'ST_X','ST_Y','ST_ENDPOINT','ST_ISCLOSED','ST_ISRING','ST_LENGTH',
            'ST_NUMPOINTS','ST_POINTN','ST_STARTPOINT','ST_AREA','ST_EXTERIORRING',
            'ST_INTERIORRINGN','ST_NUMINTERIORRINGS','ST_GEOMETRYN','ST_NUMGEOMETRIES',
            'ST_BOUNDARY','ST_BUFFER','ST_CENTROID','ST_CONTAINS','ST_CONVEXHULL',
            'ST_CROSSES','ST_DIFFERENCE','ST_DIMENSION','ST_DISJOINT','ST_DISTANCE',
            'ST_ENVELOPE','ST_EQUALS','ST_FORCE2D','ST_GEOMETRYTYPE','ST_INTERSECTION',
            'ST_INTERSECTS','ST_ISEMPTY','ST_ISSIMPLE','ST_OVERLAPS','ST_SETSRID',
            'ST_SYMDIFFERENCE','ST_TOUCHES','ST_TRANSFORM','ST_UNION','ST_WITHIN'
        ):
            dt = CLOB

        # Boolean
        elif funcName in (
            'IS_NUMBER','IS_BOOLEAN','IS_DATE','IS_DSINTERVAL','IS_YMINTERVAL','IS_TIMESTAMP',
        ):
            dt = BOOLEAN

    elif Type == 'function_scalar_cast': dt =  dataTypeFromDef(req['dataType'])
    elif Type == 'literal_exactnumeric':
        val = str(req['value'])
        size = len(val)
        scale = 0
        point = val.find('.')
        if point != -1:
            size -= 1
            scale = size - point
        dt = DataType('DECIMAL',size,scale)

    elif Type == 'literal_bool': dt = BOOLEAN
    elif Type == 'literal_double': dt = DOUBLE
    elif Type == 'literal_date': dt = DATE
    # TODO handle intervals better
    elif Type == 'literal_interval': dt = CLOB
    elif Type == 'literal_null': dt = BOOLEAN
    elif Type == 'literal_string': dt = DataType('CHAR', len(req['value']))
    elif Type in ('literal_timestamp','literal_timestamputc'): dt = TIMESTAMP
    elif Type.startswith('predicate'): dt = BOOLEAN
    else:
        raise ValueError(
            'Datatype from select list type ' + Type +
            ' not supported: ' + cjson.encode(req).encode('utf-8')
        )

    return dt


def getColumnTypeList(req, tables):
    # If there are multiple arguments to a function the
    # return type needs to be the most permissive one.
    dt = None
    for expr in req:
        thisdt = getColumnDataType(expr, tables)
        if thisdt.type == 'CHAR':
            thisdt.type = 'VARCHAR'
        if dt == None:
            dt = thisdt
        elif dt.type == thisdt.type:
            dt.scale = max(dt.scale, thisdt.scale)
            dt.size  = max(dt.intSize(), thisdt.intSize()) + dt.scale
        else:
            dts = [dt.type, thisdt.type]
            dts.sort()
            if dts == ['DECIMAL','DOUBLE']: # Double wins
                dt.type = 'DOUBLE'
                dt.size = dt.scale = 0
            else: # Any other combo becomes varchar
                dt.type = 'VARCHAR'
                dt.size = max(dt.stringSize(), thisdt.stringSize())
                dt.scale = 0
    return dt


class DataType:
    def __init__(self, type, size=0, scale=0):
        self.type  = type
        self.size  = size
        self.scale = scale

    def intSize(self):
        return self.size - self.scale

    def stringSize(self):
        if self.type == 'BOOLEAN':
            return 5 # 'False'
        elif self.type == 'DATE':
            return 10 # YYYY-MM-DD
        elif self.type == 'DECIMAL':
            return size + 2 # decimal point + sign
        elif self.type == 'DOUBLE':
            return 22 # That's what Exasol does..dunno why
        elif self.type == 'TIMESTAMP':
            return 26 # YYYY-MM-DD HH:MM:SS.FFFFFF
        elif self.type == 'VARCHAR':
            return self.type
        else:
            raise ValueError('Unhandled datatype: ' + self.type)

    def toStr(self):
        if self.type == 'DECIMAL':
            return '%s(%s,%s)' % (self.type, self.size, self.scale)
        elif self.type.endswith('CHAR'):
            return '%s(%s)' % (self.type, self.size)
        else:
            return self.type


def dataTypeFromDef(dtDef):
    return DataType(
        dtDef['type'],
        dtDef.get('size', dtDef.get('precision', 0)),
        dtDef.get('scale', 0)
    )


def getDataTypeDef(name, maxsize, prec, scale):
    if name in [ 'BOOLEAN', 'DATE', 'TIMESTAMP', 'DOUBLE' ]: dt = { 'type': name }
    elif name.startswith('CHAR'):    dt = { 'type': 'CHAR',    'size': maxsize }
    elif name.startswith('VARCHAR'): dt = { 'type': 'VARCHAR', 'size': maxsize }
    elif name.startswith('DECIMAL'): dt = { 'type': 'DECIMAL', 'precision': prec, 'scale': scale }
    else: raise ValueError('Datatype "'+name+'" not yet supported in virtual schema')
    return dt


def dataTypeDefToStr(req):
    sql = []
    Type = req.pop('type')

    if Type == 'INTERVAL':
        ft = req.pop('fromTo')
        if ft == 'YEAR TO MONTH':
            sql.append('YEAR ('+str(req.pop('precision'))+') TO MONTH')
        elif ft == 'DAY TO SECONDS':
            sql.append('DAY ('+str(req.pop('precision'))+') TO SECOND')
            sql.append('('+str(req.pop('fraction'))+')')
        else:
            raise ValueError('Unsupported interval type: '+ft)
    else:
        sql.append(Type)
        if 'size' in req:
            sql.append('(%s)' % (req.pop('size')))
        elif 'precision' in req:
            sql.append('(%s,%s)' % (req.pop('precision'), req.pop('scale')))

    if len(req) > 0:
        raise ValueError(
            'Unhandled datatype parameters: ' +
            cjson.encode(req).encode('utf-8')
        )

    return ' '.join(sql)


def getTableDef(tableName, tables):
    for table in tables:
        if table['name'] == tableName:
            return table

### END ###
