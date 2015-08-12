/*  
  plmOS Database SQLServer is a .NET library that implements a Microsoft SQL Server plmOS Database.

  Copyright (C) 2015 Processwall Limited.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published
  by the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see http://opensource.org/licenses/AGPL-3.0.
 
  Company: Processwall Limited
  Address: The Winnowing House, Mill Lane, Askham Richard, York, YO23 3NW, United Kingdom
  Tel:     +44 113 815 3440
  Email:   support@processwall.com
*/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;

namespace plmOS.Database.SQLServer
{
    internal class Table
    {
        private const String BaseItemName = "plmOS.Model.Item";

        internal Session Session { get; private set; }

        internal Model.ItemType ItemType { get; private set; }

        private String _name;
        internal String Name
        {
            get
            {
                return this._name;
            }
        }

        private Dictionary<String, Column> _columns;

        internal IEnumerable<Column> Columns
        {
            get
            {
                return this._columns.Values;
            }
        }

        internal Column Column(String Name)
        {
            return this._columns[Name];
        }

        internal Boolean HasColumn(String Name)
        {
            return this._columns.ContainsKey(Name);
        }

        private Boolean Exists { get; set; }

        private void CheckColumns()
        {
            // Load Columns already defined in Database
            this._columns = new Dictionary<String, Column>();

            String sql = "select column_name,data_type,is_nullable,character_maximum_length from information_schema.columns where table_name='" + this.Name + "'";

            using (SqlConnection connection = new SqlConnection(this.Session.Connection))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            this.Exists = true;

                            while (reader.Read())
                            {
                                // Get Max Length
                                int maxlength = -1;

                                if (!reader.IsDBNull(3))
                                {
                                    maxlength = reader.GetInt32(3);
                                }

                                // Get Primary Key
                                Boolean primarykey = false;

                                this._columns[reader.GetString(0)] = new Column(this, reader.GetString(0), reader.GetString(1), reader.GetString(2).Equals("YES"), maxlength, primarykey, true);
                            }
                        }
                        else
                        {
                            this.Exists = false;
                        }
                    }
                }
            }

            // Add Columns required by ItemType

            if (!this.HasColumn("versionid"))
            {
                this._columns["versionid"] = new Column(this, "versionid", "uniqueidentifier", false, -1, true, false);
            }

            if (this.ItemType.Name.Equals(BaseItemName))
            {
                if (!this.HasColumn("branchid"))
                {
                    this._columns["branchid"] = new Column(this, "branchid", "uniqueidentifier", false, -1, false, false);
                }

                if (!this.HasColumn("itemid"))
                {
                    this._columns["itemid"] = new Column(this, "itemid", "uniqueidentifier", false, -1, false, false);
                }

                if (!this.HasColumn("branched"))
                {
                    this._columns["branched"] = new Column(this, "branched", "bigint", false, -1, false, false);
                }

                if (!this.HasColumn("versioned"))
                {
                    this._columns["versioned"] = new Column(this, "versioned", "bigint", false, -1, false, false);
                }

                if (!this.HasColumn("superceded"))
                {
                    this._columns["superceded"] = new Column(this, "superceded", "bigint", false, -1, false, false);
                }
            }

            foreach(Model.PropertyType proptype in this.ItemType.PropertyTypes)
            {
                if (this.ItemType.Equals(proptype.ItemType))
                {
                    String colname = proptype.Name.ToLower();

                    if (!this.HasColumn(colname))
                    {
                        switch (proptype.Type)
                        {
                            case Model.PropertyTypeValues.Double:
                                this._columns[colname] = new Column(this, colname, "float", true, -1, false, false);
                                break;

                            case Model.PropertyTypeValues.String:
                                this._columns[colname] = new Column(this, colname, "nvarchar", true, ((Model.PropertyTypes.String)proptype).Length, false, false);
                                break;

                            case Model.PropertyTypeValues.Item:
                                this._columns[colname] = new Column(this, colname, "uniqueidentifier", true, -1, false, false);
                                break;
                        }
                    }
                }
            }

            if (this.Exists)
            {
                // Update Table

                foreach(Column col in this.Columns)
                {
                    if (!col.Exists)
                    {
                        // Add Column
                        String addcolsql = "alter table " + this.Name + " add " + col.Name + " " + col.Type + ";";

                        using (SqlConnection connection = new SqlConnection(this.Session.Connection))
                        {
                            connection.Open();

                            using (SqlTransaction transaction = connection.BeginTransaction())
                            {
                                using (SqlCommand command = new SqlCommand(addcolsql, connection, transaction))
                                {
                                    int res = command.ExecuteNonQuery();
                                    transaction.Commit();
                                }
                            }
                        }
                    }
                }
            }
            else
            {
                // Create Table
                String createsql = "create table " + this.Name + "(";

                foreach(Column col in this.Columns)
                {
                    createsql += col.SQL + ",";
                }

                createsql += ");";

                using (SqlConnection connection = new SqlConnection(this.Session.Connection))
                {
                    connection.Open();

                    using (SqlTransaction transaction = connection.BeginTransaction())
                    {
                        using (SqlCommand command = new SqlCommand(createsql, connection, transaction))
                        {
                            int res = command.ExecuteNonQuery();
                            transaction.Commit();
                        }
                    }
                }
            }
        }

        internal void Insert(IItem Item, Transaction Transaction)
        {
            String sql = "insert into " + this.Name;

            if (this.ItemType.Name == BaseItemName)
            {
                sql += " (versionid,branchid,itemid,branched,versioned,superceded) values ('" + Item.VersionID + "','" + Item.BranchID + "','" + Item.ItemID + "'," + Item.Branched + "," + Item.Versioned + "," + Item.Superceded + ");";
            }
            else
            {
                sql += "(versionid";
                String sqlvalues = "('" + Item.VersionID + "'";

                foreach (IProperty property in Item.Properties)
                {
                    if (property.PropertyType.ItemType.Equals(this.ItemType))
                    {
                        sql += "," + property.PropertyType.Name.ToLower();

                        if (property.Object == null)
                        {
                            sqlvalues += ",";
                        }
                        else
                        {
                            switch (property.PropertyType.Type)
                            {
                                case Model.PropertyTypeValues.Double:
                                    sqlvalues += "," + property.Object;
                                    break;
                                case Model.PropertyTypeValues.String:
                                    sqlvalues += ",'" + property.Object + "'";
                                    break;
                                case Model.PropertyTypeValues.Item:
                                    sqlvalues += ",'" + ((IItem)property.Object).BranchID + "'";
                                    break;
                            }
                        }
                    }
                }

                sql += ") values " + sqlvalues + ");";
            }

            using (SqlCommand command = new SqlCommand(sql, Transaction.SQLConnection, Transaction.SQLTransaction))
            {
                int res = command.ExecuteNonQuery();
            }
        }

        internal String ColumnsSQL
        {
            get
            {
                String sql = this.RootTableName + ".versionid," + this.RootTableName + ".branchid," + this.RootTableName + ".itemid," + this.RootTableName + ".branched," + this.RootTableName + ".versioned," + this.RootTableName + ".superceded";

                foreach(Model.PropertyType proptype in this.ItemType.PropertyTypes)
                {
                    sql += "," + this.Session.Table(proptype.ItemType).Name + "." + proptype.Name.ToLower();
                }

                return sql;
            }
        }

        internal String TablesSQL
        {
            get
            {
                if (this.ItemType.Name == BaseItemName)
                {
                    return this.Name;
                }
                else
                {
                    return this.Session.Table(this.ItemType.BaseItemType).TablesSQL + " inner join " + this.Name + " on " + this.Session.Table(this.ItemType.BaseItemType).Name + ".versionid=" + this.Name + ".versionid";
                }
            }
        }

        private String _rootTableName;
        private String RootTableName
        {
            get
            {
                if (this._rootTableName == null)
                {
                    this._rootTableName = this.Session.Table(this.ItemType.RootItemType).Name;
                }

                return this._rootTableName;
            }
        }

        private String OperatorSQL(Model.Conditions.Operators Operator)
        {
            switch (Operator)
            {
                case Model.Conditions.Operators.eq:
                    return "=";
                case Model.Conditions.Operators.ge:
                    return ">=";
                case Model.Conditions.Operators.gt:
                    return ">";
                case Model.Conditions.Operators.le:
                    return "<=";
                case Model.Conditions.Operators.lt:
                    return "<";
                case Model.Conditions.Operators.ne:
                    return "<>";
                default:
                    throw new NotImplementedException("Invalid Condition Operator: " + Operator);
            }
        }

        private String ValueSQL(Model.PropertyType PropertyType, Object Value)
        {
            if (Value == null)
            {
                return "NULL";
            }
            else
            {
                switch (PropertyType.Type)
                {
                    case Model.PropertyTypeValues.Double:
                        return Value.ToString();
                    case Model.PropertyTypeValues.Item:
                        return "'" + ((Database.IItem)Value).BranchID.ToString() + "'";

                    case Model.PropertyTypeValues.String:
                        return "'" + Value.ToString() + "'";
                    default:
                        throw new NotImplementedException("Invalid PropertyType: " + PropertyType.Type);
                }
            }
        }

        private String ConditionSQL(Model.Condition Condition)
        {
            switch (Condition.GetType().Name)
            {
                case "And":

                    String andsql = "(";

                    for (int i = 0; i < Condition.Children.Count(); i++)
                    {
                        andsql += this.ConditionSQL(Condition.Children.ElementAt(i));

                        if (i < (Condition.Children.Count() - 1))
                        {
                            andsql += " and ";
                        }
                    }

                    andsql += ")";

                    return andsql;

                case "Or":

                    String orsql = "(";

                    for (int i = 0; i < Condition.Children.Count(); i++)
                    {
                        orsql += this.ConditionSQL(Condition.Children.ElementAt(i));

                        if (i < (Condition.Children.Count() - 1))
                        {
                            orsql += " or ";
                        }
                    }

                    orsql += ")";

                    return orsql;

                case "Property":
                    Model.Conditions.Property propcondition = (Model.Conditions.Property)Condition;
                    return "(" + this.Session.Table(propcondition.PropertyType.ItemType).Name + "." + propcondition.PropertyType.Name.ToLower() + this.OperatorSQL(propcondition.Operator) + this.ValueSQL(propcondition.PropertyType, propcondition.Value) + ")";
                default:
                    throw new NotImplementedException("Condition Type not implemented: " + Condition.GetType().Name);
            }
        }

        internal IEnumerable<Database.IItem> Select(Model.Queries.Item Query) 
        {
            String sql = "select " + this.ColumnsSQL + " from " + this.TablesSQL;

            if (Query.Condition != null)
            {
                sql += " where " + this.ConditionSQL(Query.Condition);
            }

            return this.Select(sql);
        }

        internal Database.IItem Select(Guid BranchID)
        {
            String sql = "select " + this.ColumnsSQL + " from " + this.TablesSQL + " where " + this.RootTableName + ".branchid='" + BranchID + "' and " + this.RootTableName + ".superceded=-1";
            IEnumerable<Database.IItem> results = this.Select(sql);

            if (results.Count() > 0)
            {
                return results.First();
            }
            else
            {
                return null;
            }
        }

        private IEnumerable<Database.IItem> Select(String SQL)
        {
            List<Database.IItem> items = new List<Database.IItem>();

            using(SqlConnection connection = new SqlConnection(this.Session.Connection))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(SQL, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while(reader.Read())
                        {
                            Item item = new Item(this.Session);
                            item.ItemType = this.ItemType;
                            item.VersionID = reader.GetGuid(0);
                            item.BranchID = reader.GetGuid(1);
                            item.ItemID = reader.GetGuid(2);
                            item.Branched = reader.GetInt64(3);
                            item.Versioned = reader.GetInt64(4);
                            item.Superceded = reader.GetInt64(5);

                            int cnt = 6;

                            foreach(Model.PropertyType proptype in this.ItemType.PropertyTypes)
                            {
                                switch(proptype.Type)
                                {
                                    case Model.PropertyTypeValues.Double:

                                        if (reader.IsDBNull(cnt))
                                        {
                                            item.AddProperty(new Property(item, proptype, null));
                                        }
                                        else
                                        {
                                            item.AddProperty(new Property(item, proptype, reader.GetDouble(cnt)));
                                        }

                                        break;
                                    case Model.PropertyTypeValues.Item:

                                        if (reader.IsDBNull(cnt))
                                        {
                                            item.AddProperty(new Property(item, proptype, null));
                                        }
                                        else
                                        {
                                            Guid branchid = reader.GetGuid(cnt);
                                            Model.ItemType itemtype = ((Model.PropertyTypes.Item)proptype).ItemType;
                                            Database.IItem propitem = this.Session.Table(itemtype).Select(branchid);
                                            item.AddProperty(new Property(item, proptype, propitem));
                                        }

                                        break;

                                    case Model.PropertyTypeValues.String:

                                        if (reader.IsDBNull(cnt))
                                        {
                                            item.AddProperty(new Property(item, proptype, reader.GetString(cnt)));
                                        }
                                        else
                                        {
                                            item.AddProperty(new Property(item, proptype, reader.GetString(cnt)));
                                        }

                                        break;
                                }

                                cnt++;
                            }

                            items.Add(item);
                        }
                    }
                }
            }

            return items;
        }

        public override string ToString()
        {
            return this.ItemType.Name;
        }

        internal Table(Session Session, Model.ItemType ItemType)
        {
            this.Session = Session;
            this.ItemType = ItemType;

            // Set Name
            this._name = this.ItemType.Name.ToLower().Replace('.', '_');

            // Load Columns
            this.CheckColumns();
        }
    }
}
