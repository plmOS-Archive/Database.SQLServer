﻿/*  
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
    public class Session : ISession
    {
        private const String RootItemTypeName = "plmOS.Model.Item";
        private const String RootRelationshipTypeName = "plmOS.Model.Relationship";

        public String Connection { get; private set; }

        internal Model.ItemType RootItemType { get; private set; }

        internal Table RootItemTable
        {
            get
            {
                return this.TableCache[this.RootItemType];
            }
        }

        internal Model.RelationshipType RootRelationshipType { get; private set; }

        internal Table RootRelationshipTable
        {
            get
            {
                return this.TableCache[this.RootRelationshipType];
            }
        }

        private Dictionary<Model.ItemType, Table> TableCache;

        public void Create(Model.ItemType ItemType)
        {
            if (!this.TableCache.ContainsKey(ItemType))
            {
                if (ItemType.Name.Equals(RootItemTypeName))
                {
                    this.RootItemType = ItemType;
                }

                this.TableCache[ItemType] = new Table(this, ItemType);
            }
        }

        internal Table Table(Model.ItemType ItemType)
        {
            return this.TableCache[ItemType];
        }

        public void Create(Model.RelationshipType RelationshipType)
        {
            if (!this.TableCache.ContainsKey(RelationshipType))
            {
                if (RelationshipType.Name.Equals(RootRelationshipTypeName))
                {
                    this.RootRelationshipType = RelationshipType;
                }

                this.TableCache[RelationshipType] = new Table(this, RelationshipType);
            }
        }

        public ITransaction BeginTransaction()
        {
            return new Transaction(this);
        }

        private void Insert(IItem Item, Model.ItemType ItemType, Transaction Transaction)
        {
            // Insert into base table

            if (ItemType.BaseItemType != null)
            {
                this.Insert(Item, ItemType.BaseItemType, Transaction);
            }

            // Insert into Table
            this.TableCache[ItemType].Insert(Item, Transaction);
        }

        public void Create(IItem Item, ITransaction Transaction)
        {
            this.Insert(Item, Item.ItemType, (Transaction)Transaction);
        }

        public void Supercede(IItem Item, ITransaction Transaction)
        {
            String sql = "update " + this.RootItemTable.Name + " set superceded=" + Item.Superceded + " where versionid='" + Item.VersionID + "';";

            using(SqlCommand command = new SqlCommand(sql, ((Transaction)Transaction).SQLConnection, ((Transaction)Transaction).SQLTransaction))
            {
                command.ExecuteNonQuery();
            }
        }

        public IEnumerable<IItem> Get(Model.Queries.Item Query)
        {
            return this.TableCache[Query.ItemType].Select(Query);
        }

        public IEnumerable<IRelationship> Get(Model.Queries.Relationship Query)
        {
            return this.TableCache[Query.ItemType].Select(Query);
        }

        public Session(String Connection)
        {
            this.Connection = Connection;
            this.TableCache = new Dictionary<Model.ItemType, Table>();
        }
    }
}
