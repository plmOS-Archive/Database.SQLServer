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

namespace plmOS.Database.SQLServer
{
    public class Session : ISession
    {
        public String Connection { get; private set; }

        private Dictionary<Model.ItemType, Table> TableCache;

        public void Create(Model.ItemType ItemType)
        {
            if (!this.TableCache.ContainsKey(ItemType))
            {
                this.TableCache[ItemType] = new Table(this, ItemType);
                this.TableCache[ItemType].Create();
            }
        }

        public void Create(Model.RelationshipType RelationshipType)
        {
            if (!this.TableCache.ContainsKey(RelationshipType))
            {
                this.TableCache[RelationshipType] = new Table(this, RelationshipType);
                this.TableCache[RelationshipType].Create();
            }
        }

        public ITransaction BeginTransaction()
        {
            return new Transaction(this);
        }

        public void Create(IItem Item, ITransaction Transaction)
        {

        }

        public void Supercede(IItem Item, System.Int64 Time, ITransaction Transaction)
        {

        }

        public Session(String Connection)
        {
            this.Connection = Connection;
            this.TableCache = new Dictionary<Model.ItemType, Table>();
        }
    }
}
