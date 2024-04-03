function parseQuery(query) {
  // First, let's trim the query to remove any leading/trailing whitespaces
  query = query.trim();

  // Split the query at the GROUP BY clause if it exists
  const groupBySplit = query.split(/\sGROUP BY\s/i);
  const queryWithoutGroupBy = groupBySplit[0]; // Everything before GROUP BY clause

  // GROUP BY clause is the second part after splitting, if it exists
  let groupByFields = groupBySplit.length > 1 ? groupBySplit[1].trim().split(',').map(field => field.trim()) : null;

  // Split the query at the WHERE clause if it exists
  const whereSplit = queryWithoutGroupBy.split(/\sWHERE\s/i);
  const queryWithoutWhere = whereSplit[0]; // Everything before WHERE clause

  // WHERE clause is the second part after splitting, if it exists
  const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

  // Split the remaining query at the JOIN clause if it exists
  const joinSplit = queryWithoutWhere.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
  const selectPart = joinSplit[0].trim(); // Everything before JOIN clause

  // Parse the SELECT part
  const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
  const selectMatch = selectPart.match(selectRegex);
  if (!selectMatch) {
    throw new Error("Invalid SELECT format");
  }

  const [, fields, table] = selectMatch;

  const { joinType, joinTable, joinCondition } = parseJoinClause(queryWithoutWhere);

  // Parse the JOIN part if it exists

  // Parse the WHERE part if it exists
  let whereClauses = [];
  if (whereClause) {
    whereClauses = parseWhereClause(whereClause);
  }

  //   const groupByRegex = /\sGROUP BY\s(.+)/i;
  //   const groupByMatch = query.match(groupByRegex);

  const aggregateFunctionRegex = /(\bCOUNT\b|\bAVG\b|\bSUM\b|\bMIN\b|\bMAX\b)\s*\(\s*(\*|\w+)\s*\)/i;
  const hasAggregateWithoutGroupBy = aggregateFunctionRegex.test(query) && !groupByFields;
  // // Parse the WHERE part if it exists
  // let joinClause = [];
  // if (joinClause) {
  //     joinClause = parseJoinClause(joinClause);
  // }

  return {
    fields: fields.split(",").map((field) => field.trim()),
    table: table.trim(),
    whereClauses,
    joinType,
    joinTable,
    joinCondition,
    groupByFields,
    hasAggregateWithoutGroupBy,
  };
}

function parseWhereClause(whereString) {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map(conditionString => {
        const match = conditionString.match(conditionRegex);
        if (match) {
            const [, field, operator, value] = match;
            return { field: field.trim(), operator, value: value.trim() };
        }
        throw new Error('Invalid WHERE clause format');
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

module.exports = { parseQuery, parseJoinClause };
