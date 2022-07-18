package com.yun;

import cn.smallbun.screw.core.Configuration;
import cn.smallbun.screw.core.engine.EngineConfig;
import cn.smallbun.screw.core.engine.EngineFileType;
import cn.smallbun.screw.core.engine.EngineTemplateType;
import cn.smallbun.screw.core.execute.DocumentationExecute;
import cn.smallbun.screw.core.process.ProcessConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yl.z
 * @date 2022/5/26 20:51
 */
public class ScrewDemo {

    static String URL = "jdbc:oracle:thin:@127.0.0.1:1521/middledb";
    static String USER_NAME = "HDC_DWD";
    static String PASS_WORD = "XXX";
    static String FILE_NAME = "DWD表结构";
    static String DESCRIPTION = "DWD表结构";

    static String FILE_OUTPUT_DIR = "E:\\tmp\\dbwork\\";

    public static void main(String[] args) {
        documentGeneration();
    }

    /**
     * 文档生成
     */
    private static void documentGeneration() {
        //数据源
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setDriverClassName("oracle.jdbc.OracleDriver");
        hikariConfig.setJdbcUrl(URL);
        hikariConfig.setUsername(USER_NAME);
        hikariConfig.setPassword(PASS_WORD);
        //设置可以获取tables remarks信息
        hikariConfig.addDataSourceProperty("useInformationSchema", "true");
        hikariConfig.setMinimumIdle(2);
        hikariConfig.setMaximumPoolSize(5);
        DataSource dataSource = new HikariDataSource(hikariConfig);
        //生成配置
        EngineConfig engineConfig = EngineConfig.builder()
                //生成文件路径
                .fileOutputDir(FILE_OUTPUT_DIR)
                //打开目录
                .openOutputDir(true)
                //文件类型
                .fileType(EngineFileType.HTML)
                //生成模板实现
                .produceType(EngineTemplateType.freemarker)
                //自定义文件名称
                .fileName(FILE_NAME).build();

        //忽略表
        ArrayList<String> ignoreTableName = new ArrayList<>();
        ignoreTableName.add("YYH_ADDRESS_CONFIG_BAK");

        //忽略表前缀
        ArrayList<String> ignorePrefix = new ArrayList<>();
        ignorePrefix.add("_BAK");
        //忽略表后缀
        ArrayList<String> ignoreSuffix = new ArrayList<>();
        ignoreSuffix.add("_BAK");
        //根据表前缀生成
        List<String> tablePrefixLi = new ArrayList<>();
        //tablePrefixLi.add("YYH");

        ProcessConfig processConfig = ProcessConfig.builder()
                //指定生成逻辑、当存在指定表、指定表前缀、指定表后缀时
                // ，将生成指定表，其余表不生成、并跳过忽略表配置

                //根据名称指定表生成
                //.designatedTableName(new ArrayList<>())
                //根据表前缀生成
                .designatedTablePrefix(tablePrefixLi)
                //根据表后缀生成
                .designatedTableSuffix(new ArrayList<>())
                //忽略表名
                .ignoreTableName(ignoreTableName)
                //忽略表前缀
                .ignoreTablePrefix(ignorePrefix)
                //忽略表后缀
                .ignoreTableSuffix(ignoreSuffix).build();
        //配置
        Configuration config = Configuration.builder()
                //版本
                .version("1.0.0")
                //描述
                .description(DESCRIPTION)
                //数据源
                .dataSource(dataSource)
                //生成配置
                .engineConfig(engineConfig)
                //生成配置
                .produceConfig(processConfig)
                .build();
        //执行生成
        new DocumentationExecute(config).execute();
    }

}
