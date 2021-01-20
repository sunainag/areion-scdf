package com.sproutloud.starter.stream.dao.impl;

import com.sproutloud.starter.stream.dao.DedupeDao;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.support.JdbcDaoSupport;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.util.*;

/**
 * DedupeDao Interface
 *
 * @author rgupta
 */
@Slf4j
@Repository
public class DedupeDaoImpl extends JdbcDaoSupport implements DedupeDao {

    @Autowired
    private DataSource dataSource;

    /**
     * Result set extractor for query
     */
    private RowMapper<Map<String, String>> getExtractor(Set<String> columnNames) {
        RowMapper<Map<String, String>> extractor = (rs, rowNum) -> {
            Map<String, String> resultMap = new HashMap<>();
            for (String column : columnNames) {
                resultMap.put(column, rs.getString(column));
            }
            return resultMap;
        };
        return extractor;
    }

    @PostConstruct
    private void initialize() {
        setDataSource(dataSource);
    }

    /**
     * method which queries database for existing records with same dedupe_hash
     *
     * @param databaseName
     * @param tableName
     * @param dedupeHash
     * @param localityCode
     * @param listId
     * @param columnNames
     * @return Map<String, String>
     */
    @Override
    public Map<String, String> getRecordFromListData(String databaseName, String tableName, String dedupeHash,
                                                     String localityCode, String listId, Set<String> columnNames) {
        //        dedupeHash = dedupeHash.trim().toLowerCase();
        log.debug("Checking the existing entry for passed list_id: " + listId + ", locality_code: " + localityCode
                + ", dedupe_hash: " + dedupeHash);

        String columnKeyString = String.join(",", columnNames);
        StringBuffer query = new StringBuffer("SELECT ").append(columnKeyString).append(" FROM ").append(databaseName)
                .append(".").append(tableName).append(" WHERE dedupe_hash=").append("'" + dedupeHash + "'")
                .append(" AND list_id=").append("'" + listId + "'").append(" AND locality_code=")
                .append("'" + localityCode + "'").append(" LIMIT 1");
        List<Map<String, String>> resultSet = Objects.requireNonNull(getJdbcTemplate()).query(query.toString(),
                getExtractor(columnNames));
        if (!CollectionUtils.isEmpty(resultSet)) {
            log.debug("Response from database: " + resultSet);
            return resultSet.get(0);
        }
        return null;
    }
}
