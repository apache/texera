package edu.uci.ics.texera.api.constants.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import edu.uci.ics.texera.api.field.DateField;
import edu.uci.ics.texera.api.field.DoubleField;
import edu.uci.ics.texera.api.field.IField;
import edu.uci.ics.texera.api.field.IntegerField;
import edu.uci.ics.texera.api.field.StringField;
import edu.uci.ics.texera.api.field.TextField;
import edu.uci.ics.texera.api.schema.Attribute;
import edu.uci.ics.texera.api.schema.AttributeType;
import edu.uci.ics.texera.api.schema.Schema;
import edu.uci.ics.texera.api.tuple.*;

/**
 * @author Qinhua huang
 */
public class TestConstantsChineseWordCount {
    // Sample Fields
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String AGE = "age";
    public static final String HEIGHT = "height";
    public static final String DATE_OF_BIRTH = "dateOfBirth";
    public static final String DESCRIPTION = "description";

    public static final Attribute FIRST_NAME_ATTR = new Attribute(FIRST_NAME, AttributeType.STRING);
    public static final Attribute LAST_NAME_ATTR = new Attribute(LAST_NAME, AttributeType.STRING);
    public static final Attribute AGE_ATTR = new Attribute(AGE, AttributeType.INTEGER);
    public static final Attribute HEIGHT_ATTR = new Attribute(HEIGHT, AttributeType.DOUBLE);
    public static final Attribute DATE_OF_BIRTH_ATTR = new Attribute(DATE_OF_BIRTH, AttributeType.DATE);
    public static final Attribute DESCRIPTION_ATTR = new Attribute(DESCRIPTION, AttributeType.TEXT);

    // Sample Schema
    public static final Attribute[] ATTRIBUTES_PEOPLE = { FIRST_NAME_ATTR, LAST_NAME_ATTR, AGE_ATTR, HEIGHT_ATTR,
            DATE_OF_BIRTH_ATTR, DESCRIPTION_ATTR };
    public static final Schema SCHEMA_PEOPLE = new Schema(ATTRIBUTES_PEOPLE);

    public static List<Tuple> getSamplePeopleTuples() {
        
        try {
            IField[] fields1 = { new StringField("bruce"), new StringField("john Lee"), new IntegerField(46),
                    new DoubleField(5.50), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-14-1970")),
                    new TextField("中新社北京4月26日电 (记者 刘育英)“中国制造2025”政策措施实施以来，“为稳定工业增长、加快制造业转型升级发"
                            + "挥了重要作用”，效果初步显现。") };
            IField[] fields2 = { new StringField("tom hanks"), new StringField("cruise"), new IntegerField(45),
                    new DoubleField(5.95), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1971")),
                    new TextField("　　中国2015年发布了“中国制造2025”通知。中国工业和信息化部运行监测协调局副"
                            + "局长黄利斌26日在国新办新闻发布会上表示，自“中国制造2025”实施以来，国家制造业创新中心建设、智能制造"
                            + "、工业强基、绿色制造、高端装备创新等“五大工程”扎实推进；2016年度15个重大标志性项目中，7个完全落实"
                            + "，4个基本落实，其余正在推进。" ) };
            IField[] fields3 = { new StringField("brad lie angelina"), new StringField("pitt"), new IntegerField(44),
                    new DoubleField(6.10), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-12-1972")),
                    new TextField("　　2017年，工信部将重点推进六方面工作：加大“五大工程”实施力度，"
                            + "积极推进创新中心建设；扩大试点示范城市(群)覆盖面；实施新一轮重大技术改造升级工程；"+ "推进"
                            + "制造业与互联网融合发展；优化制造业发展环境。" ) };
            IField[] fields4 = { new StringField("george lin lin"), new StringField("lin clooney"), new IntegerField(43),
                    new DoubleField(6.06), new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1973")),
                    new TextField("　　黄利斌说，今年继续开展“互联"
                            + "网+”制造业试点示范，加快工业互联网基础设施改造升级。现在，47%的大企业"
                            + "搭建了运营协同创新平台，两化融合(信息化和工业化融合)管理体系贯标企业运"
                            + "营成本平均下降了8.8%，经营利润平均增长了6.9%。" ) };
            IField[] fields5 = { new StringField("christian john wayne"), new StringField("rock bale"),
                    new IntegerField(42), new DoubleField(5.99),
                    new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("　　工信部今年还将"
                            + "选取20-30个城市(群)继续开展“中国制造2025”试点示范创建，"
                            + "指导试点示范城市(群)，在落实新发展理念等方面先行先试。") };
            IField[] fields6 = { new StringField("Mary brown"), new StringField("Lake Forest"),
                    new IntegerField(42), new DoubleField(5.99),
                    new DateField(new SimpleDateFormat("MM-dd-yyyy").parse("01-13-1974")), new TextField("资料图：由驻日中"
                            + "资太阳能企业开发、承建并运营维护的日本岛根县滨田市第二期12兆瓦光伏电站项目（滨田MS太阳能发电站），4月25日在当"
                            + "地举行竣工典礼。该大型太阳能电站的九成设备来自“中国制造”。在日本并网发电的特高压太阳能电站中，这是中国产设备占"
                            + "比最高的项目。中新社记者 王健 摄") };

            Tuple tuple1 = new Tuple(SCHEMA_PEOPLE, fields1);
            Tuple tuple2 = new Tuple(SCHEMA_PEOPLE, fields2);
            Tuple tuple3 = new Tuple(SCHEMA_PEOPLE, fields3);
            Tuple tuple4 = new Tuple(SCHEMA_PEOPLE, fields4);
            Tuple tuple5 = new Tuple(SCHEMA_PEOPLE, fields5);
            Tuple tuple6 = new Tuple(SCHEMA_PEOPLE, fields6);

            return Arrays.asList(tuple1, tuple2, tuple3, tuple4, tuple5, tuple6);
//            return Arrays.asList(tuple1);
        } catch (ParseException e) {
            // exception should not happen because we know the data is correct
            e.printStackTrace();
            return Arrays.asList();
        }

    }

}
