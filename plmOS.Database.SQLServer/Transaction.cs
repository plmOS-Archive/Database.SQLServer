using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace plmOS.Database.SQLServer
{
    public class Transaction : ITransaction
    {
        public Session Session { get; private set; }

        public void Commit()
        {

        }

        public void Rollback()
        {

        }

        internal Transaction(Session Session)
        {
            this.Session = Session;
        }
    }
}
