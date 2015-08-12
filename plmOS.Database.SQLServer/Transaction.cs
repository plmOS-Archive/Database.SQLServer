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
    public class Transaction : ITransaction
    {
        public Session Session { get; private set; }

        internal SqlConnection SQLConnection { get; private set; }

        internal SqlTransaction SQLTransaction { get; private set; }

        public void Commit()
        {
            this.SQLTransaction.Commit();
        }

        public void Rollback()
        {
            this.SQLTransaction.Rollback();
        }

        public void Dispose()
        {
            this.SQLTransaction.Dispose();
            this.SQLConnection.Dispose();
        }

        internal Transaction(Session Session)
        {
            this.Session = Session;
            this.SQLConnection = new SqlConnection(this.Session.Connection);
            this.SQLConnection.Open();
            this.SQLTransaction = this.SQLConnection.BeginTransaction();
        }
    }
}
