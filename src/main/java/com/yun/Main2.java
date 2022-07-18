package com.yun;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.parser.Node;
import net.sf.jsqlparser.statement.Statement;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

import java.io.StringReader;
import java.util.List;

/**
 * @author yl.z
 * @date 2022/6/7 9:38
 */
public class Main2 {

    final static String dbType = JdbcConstants.ORACLE;

    public static void main(String[] args) throws Exception {
        String sql = "insert into hdc_dwd.dwd_patient_info\n" +
                " with staff as\n" +
                "   (select orgcode, staffcode, staffname from hdc_dim.v_dim_staff),\n" +
                "  dept as\n" +
                "   (select orgcode, deptcode, deptname from hdc_dim.v_dim_dept),\n" +
                "  ward as\n" +
                "   (select orgcode, wardcode, wardname from hdc_dim.v_dim_ward),\n" +
                "  team as\n" +
                "   (select orgcode, teamcode, teamname from hdc_dim.v_dim_team)\n" +
                "  select p.hos_code as hos_code, --机构编码\n" +
                "         p.pid as pid, --患者住院唯一号\n" +
                "         p.patient_id as patient_id, --患者唯一号\n" +
                "         p.hc_cardno as hc_card_no, --医保卡号\n" +
                "         p.visit_cardno as visit_card_no, --就诊卡号\n" +
                "         p.inpno as inp_id, --住院号\n" +
                "         p.idcard_no as id_card_no, --身份证号码\n" +
                "         p.caseid as case_id, --病案号\n" +
                "         p.name as name, --姓名\n" +
                "         p.age_desc as age, --年龄文本，19岁\n" +
                "         p.sex_name as sex_name, --性别（男/女/未知）\n" +
                "         p.birthday as birthday, --出生日期\n" +
                "         p.admiss_num as admission_num, --住院次数\n" +
                "         p.admission_date as admission_date, --入院时间\n" +
                "         p.discharge_date as discharge_date, --出院时间\n" +
                "         p.indays as in_days, --住院天数\n" +
                "         p.curr_bed_code as curr_bed_code, --床位号\n" +
                "         nvl(p.discharge_dept_code,p.curr_dept_code) as discharge_dept_code, --出院科室代码\n" +
                "         t1.deptname as discharge_dept_name, --出院科室名称\n" +
                "         nvl(p.discharge_ward_code,p.curr_ward_code) as discharge_ward_code, --出院病区代码\n" +
                "         w1.wardname as discharge_ward_name, --出院病区名称\n" +
                "         p.chief_doctor_code as chief_doctor_code, --主任医师代码\n" +
                "         s1.staffname as chief_doctor_name, --主任医师名称\n" +
                "         p.attending_doctor_code as attending_doctor_code, --主治医师代码\n" +
                "         s2.staffname as attending_doctor_name, --主治医师名称\n" +
                "         p.inp_doctor_code as inp_doctor_code, --住院医师代码\n" +
                "         s3.staffname as inp_doctor_name, --住院医师名称\n" +
                "         p.team_code as team_code, --医疗组代码\n" +
                "         m1.teamname as team_name, --医疗组名称\n" +
                "         nvl(a.zgqk, b.zgqk) as treat_condition_code, --转归情况编码\n" +
                "         nvl(a.zgqkmc, b.zgqkmc) as treat_condition_name, --1.治愈；2.好转；3.未愈；4.死亡；9.其他\n" +
                "         nvl(a.lyfs, b.lyfs) as way_discharge_code, --标准离院方式代码\n" +
                "         nvl(a.lyfsmc, b.lyfsmc) as way_discharge_name, --1.医嘱离院；2.医嘱转院；3.医嘱转社区卫生服务机构/乡镇卫生院；4.非医嘱离院；5.死亡；9.其他；\n" +
                "         nvl(a.xsecstz, b.xsecstz) as newborn_weight_birth, --患者出生体重，单位g，这里只填数值\n" +
                "         p.insurance_nature_code as insurance_nature_code, --医保类型代码\n" +
                "         p.insurance_nature_name as insurance_nature_name, --医保类型名称\n" +
                "         p.insurance_place_code as insurance_place_code, --参保地代码\n" +
                "         p.insurance_place_name as insurance_place_name, --参保地名称\n" +
                "         p.insurance_type_code as insurance_type_code, --参保类型代码\n" +
                "         p.insurance_type_name as insurance_type_name, --参保类型名称\n" +
                "         h.fee_total as fee_total, --医疗总金额，医院费用明细表中聚合得到的\n" +
                "         c.fee_insurance as fee_insurance, --医保报销金额\n" +
                "         nvl(c.fee_zili,0) + nvl(c.fee_zifei,0) + nvl(c.fee_zifu,0) as fee_zifu_all, --自付金额 = 自理 + 自费 + 自负\n" +
                "         c.fee_zili as fee_zili, --自理金额\n" +
                "         c.fee_zifei as fee_zifei, --自费金额\n" +
                "         c.fee_zifu as fee_zifu, --自负金额\n" +
                "         i.med_fee_total as fee_outmed, --出院带药费用\n" +
                "         c.settle_date as settle_date, --院内结算时间\n" +
                "         j.emr_submit_date as submit_date, --病历提交时间\n" +
                "         k.qc_date as mr_save_date, --病案保存时间\n" +
                "         (case\n" +
                "           when p.discharge_date is null then\n" +
                "            '0'\n" +
                "           when k.qc_date is not null and p.discharge_date is not null then\n" +
                "            '3'\n" +
                "           when a.pid is not null and k.qc_date is  null then\n" +
                "            '2'\n" +
                "           else\n" +
                "            '1'\n" +
                "         end) as patinet_status, --患者状态（0-在院；1-未提交；2-未编码；3-已编码）\n" +
                "         nvl(ba_sugy.surgery_date, emr_sugy.surgery_date) as main_surgery_date, --主手术时间\n" +
                "         nvl(p.discharge_date, sysdate) -\n" +
                "         nvl(ba_sugy.surgery_date, emr_sugy.surgery_date) as surgery_day, --术后天数\n" +
                "         p.is_add_point as is_add_point, --是否新技术 0-无新技术 1-达芬奇  2-飞秒\n" +
                "         '0' as is_share, --分摊标记。0-不需要分摊；1-需要分摊（分摊判断方法，单个患者有多个开单科室）\n" +
                "         (case\n" +
                "           when a.pid is not null then\n" +
                "            '1'\n" +
                "           else\n" +
                "            '0'\n" +
                "         end) as is_mr, --病案数据是否已生成。0-存在病历数据且无病案数据；1-存在病历数据且存在病案数据\n" +
                "         sysdate as modify_date, --接入时间\n" +
                "         c.fee_ins_coordinated, --统筹基金支付\n" +
                "         c.fee_ins_diag, --大病基金支付\n" +
                "         c.fee_ins_other, --其他基金支付\n" +
                "         p.drg_type, --特殊病例类型:特殊结算code统一定义。目前有0-日间手术；1-家庭病床；2-跨年结算\n" +
                "         p.is_ins,--是否医保患者  1-是;0-否\n" +
                "\t\t case when (select status from CONFIG.O_SETTLE where rownum =1) ='0' then COALESCE(emr_sugy.sign_surgery_sf,0)\n" +
                "\t\t\t  when (select status from CONFIG.O_SETTLE where rownum =1) ='1'\n" +
                "\t\t\t\t\tor (select status from CONFIG.O_SETTLE where rownum =1) is null\n" +
                "\t\t\t  then COALESCE(ba_sugy.sign_surgery_sf,0)\n" +
                "\t\t\t  else null\n" +
                "\t      end as sign_surgery_sf,--三四级手术标识\n" +
                "         p.org_code,--机构代码\n" +
                "         p.org_name, --机构名称\n" +
                "         p.hos_name,--院区名称\n" +
                "         p.hos_ins_category_code, --院内保险类型代码\n" +
                "         p.hos_ins_category_name, --院内保险类型名称\n" +
                "         p.INP_GROUP_NO, --住院组号\n" +
                "         p.MID_SETTLE_SIGN, --中途结算标记\n" +
                "     \n" +
                "         p.curr_dept_code, --当前科室代码\n" +
                "         t2.deptname, --当前科室名称\n" +
                "         p.admiss_dept_code, --入院科室代码\n" +
                "         t3.deptname --入院科室名称\n" +
                "    from hdc_dwb.inp_patient p\n" +
                "    left join (select pid, zgqk, zgqkmc, lyfs, lyfsmc, xsecstz\n" +
                "                 from hdc_dwb.inp_dwb_ba_mainmr) a\n" +
                "      on a.pid = p.pid --病案首页\n" +
                "    left join (select pid, zgqk, zgqkmc, lyfs, lyfsmc, xsecstz\n" +
                "                 from hdc_dwb.inp_dwb_emr_mainmr) b\n" +
                "      on b.pid = p.pid --病历首页\n" +
                "    left join (select pid,\n" +
                "                      sum(fee_total) fee_total,\n" +
                "                      sum(fee_insurance) fee_insurance,\n" +
                "                      sum(fee_zili) fee_zili,\n" +
                "                      sum(fee_zifei) fee_zifei,\n" +
                "                      sum(fee_zifu) fee_zifu,\n" +
                "                      max(settle_date) as settle_date,\n" +
                "                     sum(fee_ins_coordinated) as fee_ins_coordinated,\n" +
                "                    sum(fee_ins_diag) as fee_ins_diag,\n" +
                "                      sum(fee_ins_other) as fee_ins_other\n" +
                "                 from hdc_dwb.inp_settel_charge\n" +
                "                where is_delete = 0\n" +
                "                group by pid) c\n" +
                "      on c.pid = p.pid --结算信息\n" +
                "    left join (select emr.pid,\n" +
                "                      wm_concat(case\n" +
                "                                  when emr.sign_main = 0 then\n" +
                "                                   emr.icd_name || '(' || emr.icd_code || ')'\n" +
                "                                  else\n" +
                "                                   null\n" +
                "                                end) as second_surgery,\n" +
                "                      max(case\n" +
                "                            when emr.sign_main = 1 then\n" +
                "                             emr.icd_code\n" +
                "                            else\n" +
                "                             null\n" +
                "                          end) as main_surgery_code,\n" +
                "                      max(case\n" +
                "                            when emr.sign_main = 1 then\n" +
                "                             emr.icd_name\n" +
                "                            else\n" +
                "                             null\n" +
                "                          end) as main_surgery_name,\n" +
                "                      max(emr.surgery_end_date) as surgery_date,\n" +
                "\t\t\t\t\t\t\t\t\t\t\tmax(case when\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t(select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1)=1\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t and icd.std_surgery_level_code in(3,4)  then 1\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t \t\t when (\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t (select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1)=0\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\tor (select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1) is null )\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\tand icd.surgery_level_code in(3,4)  then 1\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t else null end )  sign_surgery_sf\n" +
                "                 from hdc_dwb.inp_surgery_emr emr\n" +
                "\t\t\t\t\t\t\t\t left join HDC_DIM.DIM_ICD9 icd\n" +
                "\t\t\t\t\t\t\t\t\ton icd.surgery_code=emr.surgery_code and icd.HOS_CODE = emr.HOS_CODE and icd.SURGERY_SOURCE=3 and icd.ICD_VERSION=3\n" +
                "                where is_delete = 0\n" +
                "                group by pid) emr_sugy --emr手术\n" +
                "      on emr_sugy.pid = p.pid\n" +
                "    left join (\n" +
                "select ba.pid,\n" +
                "                      wm_concat(case\n" +
                "                                  when ba.sign_main = 0 then\n" +
                "                                   ba.icd_name || '(' || ba.icd_code || ')'\n" +
                "                                  else\n" +
                "                                   null\n" +
                "                                end) as second_surgery,\n" +
                "                      max(case\n" +
                "                            when ba.sign_main = 1 then\n" +
                "                             ba.icd_code\n" +
                "                            else\n" +
                "                             null\n" +
                "                          end) as main_surgery_code,\n" +
                "                      max(case\n" +
                "                            when ba.sign_main = 1 then\n" +
                "                             ba.icd_name\n" +
                "                            else\n" +
                "                             null\n" +
                "                          end) as main_surgery_name,\n" +
                "                      max(ba.surgery_date) as surgery_date,\n" +
                "\t\t\t\t\t\t\t\t\t\t\tmax(case when (select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1) = 1 and icd.std_surgery_level_code in(3,4)  then 1\n" +
                "                               when ((select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1)=0\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t  or (select status from CONFIG.C_SWITCH where code='ICD_SURGERY_LEVEL' and rownum =1) is null )\n" +
                "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tand icd.surgery_level_code in(3,4)  then 1\n" +
                "                               else null end )  sign_surgery_sf\n" +
                "                 from hdc_dwb.inp_surgery_ba ba\n" +
                "\t\t\t\t\t\t\t\t join (select status from CONFIG.C_SWITCH where code = 'ICD_SURGERY_LEVEL' and  rownum =1) conf on 1=1\n" +
                "                 left join HDC_DIM.DIM_ICD9 icd\n" +
                "                  on icd.surgery_code=ba.surgery_code and icd.HOS_CODE = ba.HOS_CODE and icd.SURGERY_SOURCE=3 and icd.ICD_VERSION=3\n" +
                "                where is_delete = 0\n" +
                "                group by pid) ba_sugy --ba手术\n" +
                "      on ba_sugy.pid = p.pid\n" +
                "    left join (select pid, fee_total from hdc_dwd.pre_sum_patient) h\n" +
                "      on h.pid = p.pid --费用预聚合\n" +
                "    left join (select pid, sum(med_fee_total) as med_fee_total\n" +
                "                 from hdc_dwb.inp_outmed_detail\n" +
                "                group by pid) i\n" +
                "      on i.pid = p.pid --出院带药\n" +
                "    left join (select pid, emr_submit_date from hdc_dwb.inp_case_filed) j\n" +
                "      on j.pid = p.pid --病历提交状态\n" +
                "    left join (select pid, qc_date from hdc_dwb.inp_case_lock) k\n" +
                "      on k.pid = p.pid --病案编码状态\n" +
                "    left join staff s1\n" +
                "      on s1.staffcode = p.chief_doctor_code\n" +
                "     --and s1.orgcode = p.hos_code\n" +
                "    left join staff s2\n" +
                "      on s2.staffcode = p.attending_doctor_code\n" +
                "     --and s2.orgcode = p.hos_code\n" +
                "    left join staff s3\n" +
                "      on s3.staffcode = p.inp_doctor_code\n" +
                "     --and s3.orgcode = p.hos_code\n" +
                "    left join dept t1\n" +
                "      on t1.deptcode = nvl(p.discharge_dept_code,p.curr_dept_code)\n" +
                "    left join dept t2\n" +
                "      on t2.deptcode = p.curr_dept_code    \n" +
                "    left join dept t3\n" +
                "      on t3.deptcode = p.admiss_dept_code\n" +
                "     --and t1.orgcode = p.hos_code\n" +
                "    left join ward w1\n" +
                "      on w1.wardcode = nvl(p.discharge_ward_code,p.curr_ward_code)\n" +
                "    left join team m1\n" +
                "      on m1.teamcode = p.team_code\n" +
                "         where  p.modify_date>= to_date('20220420','yyyyMMdd')" +
                ";select 1 from dual";

        //sql = "select 1 from dual";
        CCJSqlParserManager ccm = new CCJSqlParserManager();
        Statement parse1 = ccm.parse(new StringReader(sql));
        Statement parse = CCJSqlParserUtil.parse(sql);
        Node node = CCJSqlParserUtil.parseAST(sql);
        System.out.println("");

        /*SqlParser.Config config = SqlParser.configBuilder().setLex(Lex.ORACLE).build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNodeList sqlNodes = sqlParser.parseStmtList();
        System.out.println("");*/
        /*TGSqlParser sqlParser = new TGSqlParser(EDbVendor.dbvoracle);
        sqlParser.sqltext = sql;
        sqlParser.parse();
        TStatementList sqlstatements = sqlParser.getSqlstatements();*/
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);
        List<SQLStatement> sqlStatementList = SQLUtils.toStatementList(sql, dbType);
        System.out.println("");
    }

}
