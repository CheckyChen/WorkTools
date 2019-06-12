using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Data.SqlClient;
using Newtonsoft.Json;
using 主索引数据字典导入工具.Entity;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Threading.Tasks;

namespace 主索引数据字典导入工具
{
    public partial class Form1 : Form
    {

        private static string sqlTemplate = "select {0},{1} from {2} where {3} = '{4}' order by {1}";
        public Form1()
        {
            System.Windows.Forms.Control.CheckForIllegalCrossThreadCalls = false;
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            txtResult.Text = "";
            txtResult.Text += DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " - 开始导入...\r\n";
            button1.Enabled = false;
            var typeArr = new string[] {"country", "abo_blood_type" , "nation","marriage", "occupation","idtype",
                "rh_blood_type","relationship","gender","degree"};
            foreach (var typeName in typeArr)
            {
                importDict(typeName);
            }
            button1.Enabled = true;
            txtResult.Text += DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " - 导入完毕！";
        }

        private void importDict(string type)
        {
            try
            {
                var typeId = GetValueByControlName(type);
                if (!string.IsNullOrEmpty(typeId))
                {
                    var sbSql = new StringBuilder();
                    var code = txtDictCodeField.Text;
                    var name = txtDictNameField.Text;
                    var table = txtDictTableName.Text;
                    var typeIDField = txtDictTypeID.Text;

                    sbSql.AppendFormat(sqlTemplate, code, name, table, typeIDField, typeId);
                    var dt = new SqlHelper(txtSQLConnection.Text).ExecuteDataTable(sbSql.ToString());

                    if (dt.Rows.Count > 0)
                    {
                        var json = JsonConvert.SerializeObject(dt);
                        var mongodb = new MongoDBHelper(txtMongodbCon.Text, txtMongoCollection.Text);
                        var dict = new DictEntity();
                        dict.type = type;
                        dict.data = new List<BsonDocument>();
                        foreach (DataRow dr in dt.Rows)
                        {
                            var doc = new BsonDocument();
                            doc.Set("code", dr[code].ToString());
                            doc.Set("name", dr[name].ToString());
                            dict.data.Add(doc);
                        }
                        var documentName = "DC_DICTION";
                        var filter = new BsonDocument();
                        filter.Set("type", type);
                        var docs = mongodb.GetDocumentsByFilter<DictEntity>(documentName, filter);
                        if (!docs.Any() || docs.First().data.Count < dt.Rows.Count)
                        {
                            mongodb.DeleteMany<DictEntity>(documentName, filter);
                            mongodb.Insert<DictEntity>(documentName, dict);
                            txtResult.Text += DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " - 【" + type + "】数据导入成功，导入" + dt.Rows.Count + "条\r\n";
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                txtResult.Text += DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + " - 【" + type + "】导入失败，" + ex.Message + "\r\n";
            }
        }


        private string GetValueByControlName(string name)
        {
            var ctrls = this.Controls;
            var val = string.Empty;
            foreach (var ctrl in ctrls)
            {
                if (ctrl is TextBox)
                {
                    var txtCtrl = ctrl as TextBox;
                    if (txtCtrl.Name == name)
                    {
                        val = txtCtrl.Text;
                        break;
                    }
                }
            }
            return val;
        }
    }
}
