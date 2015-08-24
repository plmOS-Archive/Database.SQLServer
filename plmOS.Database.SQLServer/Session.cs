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
using System.IO;

namespace plmOS.Database.SQLServer
{
    public class Session : ISession
    {
        private const String RootItemTypeName = "plmOS.Model.Item";
        private const String RootFileTypeName = "plmOS.Model.File";
        private const String RootRelationshipTypeName = "plmOS.Model.Relationship";

        public String Connection { get; private set; }

        private DirectoryInfo _vaultDirectory;
        public DirectoryInfo VaultDirectory 
        { 
            get
            {
                return this._vaultDirectory;
            }
            private set
            {
                this._vaultDirectory = value;

                // Ensure Vault Directory Exists
                if (!this._vaultDirectory.Exists)
                {
                    this._vaultDirectory.Create();
                }
            }
        }

        public Boolean Reading
        {
            get
            {
                return false;
            }
        }

        public Boolean Writing
        {
            get
            {
                return false;
            }
        }

        private Boolean _initialised;
        public Boolean Initialised
        {
            get
            {
                return this._initialised;
            }
            private set
            {
                this._initialised = value;

                if (this._initialised)
                {
                    if (this.InitialseCompleted != null)
                    {
                        this.InitialseCompleted(this, new EventArgs());
                    }
                }
            }
        }

        public event EventHandler InitialseCompleted;

        internal Model.ItemType RootItemType { get; private set; }

        internal Table RootItemTable
        {
            get
            {
                return this.TableCache[this.RootItemType];
            }
        }

        internal Model.ItemType RootFileType { get; private set; }

        internal Model.RelationshipType RootRelationshipType { get; private set; }

        internal Table RootRelationshipTable
        {
            get
            {
                return this.TableCache[this.RootRelationshipType];
            }
        }

        private Dictionary<Model.ItemType, Table> TableCache;

        private Dictionary<Guid, Model.ItemType> ItemTypeCache;

        public Model.ItemType ItemType(Guid ID)
        {
            return this.ItemTypeCache[ID];
        }

        public void Create(Model.ItemType ItemType)
        {
            if (!this.TableCache.ContainsKey(ItemType))
            {
                if (ItemType.Name.Equals(RootItemTypeName))
                {
                    this.RootItemType = ItemType;
                }
                else if (ItemType.Name.Equals(RootFileTypeName))
                {
                    this.RootFileType = ItemType;
                }

                this.TableCache[ItemType] = new Table(this, ItemType);
                this.ItemTypeCache[ItemType.ID] = ItemType;
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
                this.ItemTypeCache[RelationshipType.ID] = RelationshipType;
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

        public void Create(IRelationship Relationship, ITransaction Transaction)
        {
            this.Insert(Relationship, Relationship.ItemType, (Transaction)Transaction);
        }

        public void Create(IFile File, ITransaction Transaction)
        {
            this.Insert(File, File.ItemType, (Transaction)Transaction);
        }

        public void Supercede(IItem Item, ITransaction Transaction)
        {
            String sql = "update " + this.RootItemTable.Name + " set superceded=" + Item.Superceded + " where versionid='" + Item.VersionID + "';";

            using(SqlCommand command = new SqlCommand(sql, ((Transaction)Transaction).SQLConnection, ((Transaction)Transaction).SQLTransaction))
            {
                command.ExecuteNonQuery();
            }
        }

        public IItem Get(Model.ItemType ItemType, Guid BranchID)
        {
            return this.TableCache[ItemType].Select(BranchID);
        }

        public IEnumerable<IItem> Get(Model.Queries.Item Query)
        {
            return this.TableCache[Query.ItemType].Select(Query);
        }

        public IEnumerable<IRelationship> Get(Model.Queries.Relationship Query)
        {
            return this.TableCache[Query.ItemType].Select(Query);
        }

        private FileInfo VaultFile(IFile File)
        {
            return new FileInfo(this.VaultDirectory.FullName + "\\" + File.VersionID + ".dat");
        }

        public FileStream ReadFromVault(IFile File)
        {
            FileInfo vaultfile = this.VaultFile(File);
            return new FileStream(vaultfile.FullName, FileMode.Open);
        }

        public FileStream WriteToVault(IFile File) 
        {
            FileInfo vaultfile = this.VaultFile(File);
            return new FileStream(vaultfile.FullName, FileMode.Create);
        }

        public void Dispose()
        {

        }

        public Session(String Connection, DirectoryInfo VaultDirectory)
        {
            this.Connection = Connection;
            this.VaultDirectory = VaultDirectory;
            this.TableCache = new Dictionary<Model.ItemType, Table>();
            this.ItemTypeCache = new Dictionary<Guid, Model.ItemType>();
            this.Initialised = true;
        }
    }
}
