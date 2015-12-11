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
        internal Session Session { get; private set; }

        internal Model.ItemType RootItemType
        {
            get
            {
                return this.Session.RootItemType;
            }
        }

        internal Table RootItemTable
        {
            get
            {
                return this.Session.RootItemTable;
            }
        }

        private String RootItemTableName
        {
            get
            {
                return this.RootItemTable.Name;
            }
        }

        internal Model.RelationshipType RootRelationshipType
        {
            get
            {
                return this.Session.RootRelationshipType;
            }
        }

        internal Table RootRelationshipTable
        {
            get
            {
                return this.Session.RootRelationshipTable;
            }
        }

        private String RootRelationshipTableName
        {
            get
            {
                return this.RootRelationshipTable.Name;
            }
        }

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

            if (this.ItemType.Equals(this.RootItemType))
            {
                if (!this.HasColumn("branchid"))
                {
                    this._columns["branchid"] = new Column(this, "branchid", "uniqueidentifier", false, -1, false, false);
                }

                if (!this.HasColumn("itemid"))
                {
                    this._columns["itemid"] = new Column(this, "itemid", "uniqueidentifier", false, -1, false, false);
                }

                if (!this.HasColumn("itemtypeid"))
                {
                    this._columns["itemtypeid"] = new Column(this, "itemtypeid", "uniqueidentifier", false, -1, false, false);
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
            else if (this.ItemType.Equals(this.RootRelationshipType))
            {
                if (!this.HasColumn("parentbranchid"))
                {
                    this._columns["parentbranchid"] = new Column(this, "parentbranchid", "uniqueidentifier", false, -1, false, false);
                }
            }
            else
            {
                foreach (Model.PropertyType proptype in this.ItemType.PropertyTypes)
                {
                    if (!proptype.IsInherited)
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

                                case Model.PropertyTypeValues.DateTime:
                                    this._columns[colname] = new Column(this, colname, "datetime", true, -1, false, false);
                                    break;

                                case Model.PropertyTypeValues.List:
                                    this._columns[colname] = new Column(this, colname, "int", true, -1, false, false);
                                    break;

                                case Model.PropertyTypeValues.Boolean:
                                    this._columns[colname] = new Column(this, colname, "bit", true, -1, false, false);
                                    break;

                                default:
                                    throw new NotImplementedException("PropertyType not implemented: " + proptype.Type);
                            }
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
                        String addcolsql = "alter table " + this.Name + " add [" + col.Name + "] " + col.Type + ";";

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

            if (this.ItemType.Equals(this.RootItemType))
            {
                sql += " (versionid,branchid,itemid,itemtypeid,branched,versioned,superceded) values ('" + Item.VersionID + "','" + Item.BranchID + "','" + Item.ItemID + "','" + Item.ItemType.ID + "'," + Item.Branched + "," + Item.Versioned + "," + Item.Superceded + ");";
            }
            else if (this.ItemType.Equals(this.RootRelationshipType))
            {
                sql += " (versionid,parentbranchid) values ('" + Item.VersionID + "','" + ((Database.IRelationship)Item).ParentBranchID + "');";
            }
            else
            {
                sql += " (versionid";
                String sqlvalues = "('" + Item.VersionID + "'";

                foreach (Model.PropertyType proptype in this.ItemType.PropertyTypes)
                {
                    if (!proptype.IsInherited)
                    {
                        IProperty property = Item.Property(proptype);
                        sql += "," + property.PropertyType.Name.ToLower();

                        sqlvalues += "," + this.ValueSQL(proptype, property.Object);
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
                String sql = this.RootItemTableName + ".versionid," + this.RootItemTableName + ".branchid," + this.RootItemTableName + ".itemid," + this.RootItemTableName + ".itemtypeid," + this.RootItemTableName + ".branched," + this.RootItemTableName + ".versioned," + this.RootItemTableName + ".superceded";

                if (this.ItemType.IsSubclassOf(this.RootRelationshipType))
                {
                    sql += "," + this.RootRelationshipTableName + ".parentbranchid";
                }

                foreach(Model.PropertyType proptype in this.ItemType.PropertyTypes)
                {
                    sql += "," + this.Session.Table(proptype.DefinitionItemType).Name + "." + proptype.Name.ToLower();
                }

                return sql;
            }
        }

        internal String TablesSQL
        {
            get
            {
                if (this.ItemType.Equals(this.RootItemType))
                {
                    return this.Name;
                }
                else
                {
                    return this.Session.Table(this.ItemType.BaseItemType).TablesSQL + " inner join " + this.Name + " on " + this.Session.Table(this.ItemType.BaseItemType).Name + ".versionid=" + this.Name + ".versionid";
                }
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
                    case Model.PropertyTypeValues.List:
                        return Value.ToString();
                    case Model.PropertyTypeValues.Item:

                        if (Value is plmOS.Model.Item)
                        {
                            return "'" + ((plmOS.Model.Item)Value).BranchID.ToString() + "'";
                        }
                        else
                        {
                            return "'" + Value.ToString() + "'";
                        }

                    case Model.PropertyTypeValues.String:
                        return "'" + Value.ToString().Replace("'", "''") + "'";
                    case Model.PropertyTypeValues.DateTime:
                        return "'" + ((System.DateTime)Value).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fff") + "'";
                    case Model.PropertyTypeValues.Boolean:

                        if ((Boolean)Value)
                        {
                            return "1";
                        }
                        else
                        {
                            return "0";
                        }

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
                    Model.PropertyType proptype = this.ItemType.PropertyType(propcondition.Name);
                    return "(" + this.Session.Table(proptype.ItemType).Name + "." + proptype.Name.ToLower() + this.OperatorSQL(propcondition.Operator) + this.ValueSQL(proptype, propcondition.Value) + ")";
                default:
                    throw new NotImplementedException("Condition Type not implemented: " + Condition.GetType().Name);
            }
        }

        internal IEnumerable<Database.IItem> Select(Model.Queries.Item Query) 
        {
            String sql = "select " + this.ColumnsSQL + " from " + this.TablesSQL;

            if (Query.Condition != null)
            {
                sql += " where ((" + this.RootItemTableName + ".superceded=-1) and (" + this.ConditionSQL(Query.Condition) + "))";
            }
            else
            {
                sql += " where (" + this.RootItemTableName + ".superceded=-1)";
            }

            return this.SelectItems(sql);
        }

        internal Database.IItem Select(Guid BranchID)
        {
            String sql = "select " + this.ColumnsSQL + " from " + this.TablesSQL + " where ((" + this.RootItemTableName + ".branchid='" + BranchID + "') and (" + this.RootItemTableName + ".superceded=-1))";
            IEnumerable<Database.IItem> results = this.SelectItems(sql);

            if (results.Count() > 0)
            {
                return results.First();
            }
            else
            {
                return null;
            }
        }

        internal IEnumerable<Database.IRelationship> Select(Model.Queries.Relationship Query)
        {
            String sql = "select " + this.ColumnsSQL + " from " + this.TablesSQL;

            if (Query.Condition != null)
            {
                sql += " where ((" + this.RootItemTableName + ".superceded=-1) and (" + this.RootRelationshipTableName + ".parentbranchid='" + Query.Parent.BranchID + "') and (" + this.ConditionSQL(Query.Condition) + "))";
            }
            else
            {
                sql += " where ((" + this.RootItemTableName + ".superceded=-1) and (" + this.RootRelationshipTableName + ".parentbranchid='" + Query.Parent.BranchID + "'))";
            }

            return this.SelectRelationships(sql);
        }

        private void SetItemProperties(Item Item, SqlDataReader Reader, int StartIndex)
        {
            int cnt = StartIndex;

            foreach (Model.PropertyType proptype in this.ItemType.PropertyTypes)
            {
                switch (proptype.Type)
                {
                    case Model.PropertyTypeValues.Boolean:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Item.AddProperty(new Property(Item, proptype, Reader.GetBoolean(cnt)));
                        }

                        break;

                    case Model.PropertyTypeValues.Double:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Item.AddProperty(new Property(Item, proptype, Reader.GetDouble(cnt)));
                        }

                        break;
                    case Model.PropertyTypeValues.Item:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Guid branchid = Reader.GetGuid(cnt);
                            Model.ItemType itemtype = ((Model.PropertyTypes.Item)proptype).PropertyItemType;
                            Item.AddProperty(new Property(Item, proptype, branchid));
                        }

                        break;

                    case Model.PropertyTypeValues.String:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Item.AddProperty(new Property(Item, proptype, Reader.GetString(cnt)));
                        }

                        break;

                    case Model.PropertyTypeValues.DateTime:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Item.AddProperty(new Property(Item, proptype, Reader.GetDateTime(cnt).ToLocalTime()));
                        }

                        break;

                    case Model.PropertyTypeValues.List:

                        if (Reader.IsDBNull(cnt))
                        {
                            Item.AddProperty(new Property(Item, proptype, null));
                        }
                        else
                        {
                            Item.AddProperty(new Property(Item, proptype, Reader.GetInt32(cnt)));
                        }

                        break;

                    default:
                        throw new NotImplementedException("PropertyType not implemented: " + proptype.Type);
                }

                cnt++;
            }
        }

        private void SetItemCommonProperties(Item Item, SqlDataReader Reader)
        {
            Item.VersionID = Reader.GetGuid(0);
            Item.BranchID = Reader.GetGuid(1);
            Item.ItemID = Reader.GetGuid(2);
            Item.ItemType = this.Session.ItemType(Reader.GetGuid(3));
            Item.Branched = Reader.GetInt64(4);
            Item.Versioned = Reader.GetInt64(5);
            Item.Superceded = Reader.GetInt64(6);
        }

        private void SetRelationshipProperties(Relationship Relationship, SqlDataReader Reader)
        {
            this.SetItemCommonProperties(Relationship, Reader);
            Relationship.ParentBranchID = Reader.GetGuid(7);
            this.SetItemProperties(Relationship, Reader, 8);
        }

        private void SetItemProperties(Item Item, SqlDataReader Reader)
        {
            this.SetItemCommonProperties(Item, Reader);
            this.SetItemProperties(Item, Reader, 7);
        }

        private IEnumerable<Database.IItem> SelectItems(String SQL)
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
                            if (this.ItemType.Equals(this.Session.RootFileType))
                            {
                                File item = new File(this.Session);
                                this.SetItemProperties(item, reader);
                                items.Add(item);
                            }
                            else
                            {
                                Item item = new Item(this.Session);
                                this.SetItemProperties(item, reader);
                                items.Add(item);
                            }
                        }
                    }
                }
            }

            return items;
        }

        private IEnumerable<Database.IRelationship> SelectRelationships(String SQL)
        {
            List<Database.IRelationship> items = new List<Database.IRelationship>();

            using (SqlConnection connection = new SqlConnection(this.Session.Connection))
            {
                connection.Open();

                using (SqlCommand command = new SqlCommand(SQL, connection))
                {
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            Relationship item = new Relationship(this.Session);
                            this.SetRelationshipProperties(item, reader);
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
