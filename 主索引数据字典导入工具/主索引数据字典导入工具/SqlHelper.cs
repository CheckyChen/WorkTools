using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;
using System.Configuration;

namespace 主索引数据字典导入工具
{
    public class SqlHelper
    {
        private string strConnectionString = "";
        public static SqlConnection cnn;

        /// <summary>
        /// 初始化构造函数
        /// </summary>
        public SqlHelper(string conStr)
        {
            strConnectionString = conStr;
        }

        /// <summary>
        ///打开数据库连接
        /// </summary>
        public void Open()
        {
            if (cnn == null)
            {
                //cnn = new SqlConnection(ConfigurationManager.AppSettings["Conn"]);
                cnn = new SqlConnection(strConnectionString);

            }
            if (cnn.State == ConnectionState.Closed)
            {
                try
                {
                    cnn.Open();
                }
                catch (Exception ex)
                {
                    throw new Exception(ex.Message);
                }

            }
        }

        /// <summary>
        /// 关闭数据库连接
        /// </summary>
        public void close()
        {
            if (cnn != null)
            {
                if (cnn.State == ConnectionState.Open)
                {
                    cnn.Close();
                }
            }

        }

        /// <summary>
        /// 释放资源
        /// </summary>
        public void Dispose()
        {
            // 确认连接是否已经关闭
            if (cnn != null)
            {
                cnn.Dispose();

                cnn = null;
            }
        }

        /// <summary>
        /// 执行添加、更新、删除等Sql语句
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <returns>返回受影响的行数</returns>
        public int ExecuteNonQuery(string query)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("INSERT") | query.StartsWith("insert") | query.StartsWith("UPDATE") | query.StartsWith("update") | query.StartsWith("DELETE") | query.StartsWith("delete"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            int retval;
            try
            {
                cnn.Open();
                retval = cmd.ExecuteNonQuery();
                cnn.Close();
            }
            catch (Exception exp)
            {
                throw exp;
            }
            finally
            {
                if (cnn.State == ConnectionState.Open)
                {
                    cnn.Close();
                }
            }
            return retval;
        }

        /// <summary>
        /// 执行添加、更新、删除等Sql语句
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <param name="parameters">参数</param>
        /// <returns>返回受影响的行数</returns>
        public int ExecuteNonQuery(string query, params SqlParameter[] parameters)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("INSERT") | query.StartsWith("insert") | query.StartsWith("UPDATE") | query.StartsWith("update") | query.StartsWith("DELETE") | query.StartsWith("delete"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            for (int i = 0; i <= parameters.Length - 1; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
            cnn.Open();
            int retval = cmd.ExecuteNonQuery();
            cnn.Close();
            return retval;
        }
        public Exception Execute(string sql)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(sql, cnn);
            cmd.Connection.Open();
            SqlTransaction tran = cmd.Connection.BeginTransaction();
            cmd.Transaction = tran;
            Exception exret = null;
            try
            {
                cmd.ExecuteNonQuery();
                tran.Commit();
            }
            catch (Exception ex)
            {
                tran.Rollback();
                exret = ex;
            }
            finally
            {
                cmd.Connection.Close();
            }
            return exret;
        }
        /// <summary>
        /// 查询第一行第一列
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <returns></returns>
        public object ExecuteScalar(string query)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            cnn.Open();
            object retval = cmd.ExecuteNonQuery();
            cnn.Close();
            return retval;
        }

        /// <summary>
        /// 查询第一行第一列
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <param name="parameters">参数</param>
        /// <returns></returns>
        public object ExecuteScalar(string query, params SqlParameter[] parameters)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            for (int i = 0; i <= parameters.Length - 1; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
            cnn.Open();
            object retval = cmd.ExecuteScalar();
            cnn.Close();
            return retval;
        }

        /// <summary>
        /// Reader数据库查询
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <returns>返回SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string query)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
                cnn.Open();
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
                cnn.Open();
            }
            SqlDataReader dr;
            try
            {
                dr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return dr;
            }
            catch (Exception ee)
            {
                cnn.Close();
                throw ee;
            }

        }

        /// <summary>
        /// Reader数据库查询
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <param name="parameters">参数</param>
        /// <returns>返回SqlDataReader</returns>
        public SqlDataReader ExecuteReader(string query, params SqlParameter[] parameters)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            for (int i = 0; i <= parameters.Length - 1; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
            cnn.Open();
            return cmd.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>
        /// DataSet离线数据库保存数据
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <returns>返回DataSet</returns>
        public DataSet ExecuteDataSet(string query)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            SqlDataAdapter da = new SqlDataAdapter();
            da.SelectCommand = cmd;
            DataSet ds = new DataSet();
            da.Fill(ds);
            return ds;
        }

        /// <summary>
        /// DataSet离线数据库保存数据
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <param name="parameters">参数</param>
        /// <returns>返回DataSet</returns>
        public DataSet ExecuteDataSet(string query, params SqlParameter[] parameters)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            for (int i = 0; i <= parameters.Length - 1; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
            SqlDataAdapter da = new SqlDataAdapter();
            da.SelectCommand = cmd;
            DataSet ds = new DataSet();
            da.Fill(ds);
            return ds;
        }

        /// <summary>
        /// DataTable离线数据库保存数据
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <returns>返回DataSet</returns>
        public DataTable ExecuteDataTable(string query)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            SqlDataAdapter da = new SqlDataAdapter();
            da.SelectCommand = cmd;
            DataTable dt = new DataTable();
            da.Fill(dt);
            return dt;
        }

        /// <summary>
        /// DataTable离线数据库保存数据
        /// </summary>
        /// <param name="query">sql语句或者存储过程</param>
        /// <param name="parameters">参数</param>
        /// <returns>返回DataSet</returns>
        public DataTable ExecuteDataTable(string query, params SqlParameter[] parameters)
        {
            cnn = new SqlConnection(strConnectionString);
            SqlCommand cmd = new SqlCommand(query, cnn);
            if (query.StartsWith("SELECT") | query.StartsWith("select"))
            {
                cmd.CommandType = CommandType.Text;
            }
            else
            {
                cmd.CommandType = CommandType.StoredProcedure;
            }
            for (int i = 0; i <= parameters.Length - 1; i++)
            {
                cmd.Parameters.Add(parameters[i]);
            }
            SqlDataAdapter da = new SqlDataAdapter();
            da.SelectCommand = cmd;
            DataTable dt = new DataTable();
            da.Fill(dt);
            return dt;
        }

        /// <summary>
        /// 执行多条SQL语句，实现数据库事务。
        /// </summary>
        /// <param name="SQLStringList">多条SQL语句</param>		
        public int ExecuteSqlTran(List<String> SQLStringList)
        {
            using (SqlConnection conn = new SqlConnection(strConnectionString))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = conn;
                SqlTransaction tx = conn.BeginTransaction();
                cmd.Transaction = tx;
                try
                {
                    int count = 0;
                    for (int n = 0; n < SQLStringList.Count; n++)
                    {
                        string strsql = SQLStringList[n];
                        if (strsql.Trim().Length > 1)
                        {
                            cmd.CommandText = strsql;
                            count += cmd.ExecuteNonQuery();
                        }
                    }
                    tx.Commit();
                    return count;
                }
                catch
                {
                    tx.Rollback();
                    return 0;
                }
            }
        }

        /// <summary>
        /// 执行一条计算查询结果语句，返回查询结果（object）。
        /// </summary>
        /// <param name="SQLString">计算查询结果语句</param>
        /// <returns>查询结果（object）</returns>
        public object GetSingle(string SQLString)
        {
            using (SqlConnection connection = new SqlConnection(strConnectionString))
            {
                using (SqlCommand cmd = new SqlCommand(SQLString, connection))
                {
                    try
                    {
                        connection.Open();
                        object obj = cmd.ExecuteScalar();
                        if ((Object.Equals(obj, null)) || (Object.Equals(obj, System.DBNull.Value)))
                        {
                            return null;
                        }
                        else
                        {
                            return obj;
                        }
                    }
                    catch (System.Data.SqlClient.SqlException e)
                    {
                        connection.Close();
                        throw e;
                    }
                }
            }
        }
    }
}