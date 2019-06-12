using MongoDB.Bson;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace 主索引数据字典导入工具.Entity
{
    /*********************************
     * 版    权：Copyright (c) 2019 恺恩泰(北京)科技有限公司
     * 创建时间：2019/06/10 16:48:48
     * 创 建 人：陈宗焰
     * 描    述：
     **********************************/
    public class DictEntity
    {
        public ObjectId _id { get; set; }
        public string type { get; set; }
        public List<BsonDocument> data { get; set; }
    }
}
