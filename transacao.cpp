#include <iostream>
#include <stdio.h>
#include <libpq-fe.h>
#include <cstring>
#include <stdlib.h>
#include <string.h>
// #include <pqxx/pqxx>

/*Objeto de conexão*/
PGconn *conn = NULL;
/*Ponteiro de resultado*/
PGresult *result;


// char insertWithCommit(char sql){
//    return false;
// }

// char insert(char sql){
//    return true;
// }

int mountSQL(){
   FILE *arq; 
   arq = fopen("data.csv", "r"); 
   int buffer_size = 1024;
   char buffer[1024];
   int cont = 0;

   while (fgets(buffer, buffer_size, arq))
   {
      cont++;
      char sql[buffer_size] = "INSERT INTO product(description) VALUES ('";
      char str2[buffer_size] = "');";
      strcat(sql, buffer);
      strcat(sql, str2);

      printf("\n%s\n", sql);
      
      // insertWithCommit(*sql);
      // insert(sql);

   }
   printf("Total de tuplas \n%d\n", cont);
   fclose(arq);
   return true;
}


int main()
{

   /*realiza a conexão*/
   conn = PQconnectdb("host=localhost dbname=TESTE user=vini password=1234 hostaddr=127.0.0.1 port=5432");
    
   if(PQstatus(conn) == CONNECTION_OK)
   {
      printf("Conexão efetuada com sucesso. \n");

      mountSQL();
   
   }
   else{
      printf("testando falha ");   
      printf("Falha na conexão. Erro: %s", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
   }
    
    /*Verifica se a conexão está aberta e a encerra*/
   if(conn != NULL)
      PQfinish(conn);
}
