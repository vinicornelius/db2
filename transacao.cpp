// #  UNIVERSIDADE DA FRONTEIRA SUL
// #  TRABALHO DE BANCO DE DADOR - TP1
// #  Autores: Vinicius Cornelius -1811100035 e Leonardo Soares 1811100038  

// #  Tempo de execução com rollback com as 10.000 tuplas  0.98s
// #  Tempo de execução de inserção  1.318483 min 

// #  Conclusão: Não encontramos o auto commit para a biblioteca libpq-fe que foi 
// #  usada para realizar a conexão com o banco de dados, portanto toda vez que é 
// #  realizado um insert, logo em seguido o acontece um commit. Scima estão os tempos
// #  de execução.


#include <iostream>
#include <stdio.h>
#include <libpq-fe.h>
#include <cstring>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// /Objeto de conexão/
PGconn *conn = NULL;
// /Ponteiro de resultado/
PGresult *result;

char insertWithErrosRollback(){
   PQexec(conn, "INSERT INTO product(eid, description) VALUES (1, stethoscope)");
   return true;
}

char explicitTransaction(char buffer[], int buffer_size, FILE *arq, int cont){
   
   //monitorar o tempo de execução
   clock_t tempo;
   tempo = clock();

   result = PQexec(conn, "BEGIN");
   // if (PQresultStatus(result) != PGRES_COMMAND_OK)
   //  {
   //      printf("BEGIN command failed: %s", PQerrorMessage(conn));
   //      PQclear(result);
   //  }

   printf("BEGIN START\n");
   while (fgets(buffer, buffer_size, arq))
   {
      cont++;
      char sql[buffer_size] = "INSERT INTO product(eid, description) VALUES (";
      char str2[buffer_size] = ", '";
      char str3[buffer_size] = "');";
      char szBuffer[1024];

      //printf("\n%d\n", cont);
      snprintf(szBuffer, sizeof (szBuffer), "%d",cont);
      strcat(sql, szBuffer);
      strcat(sql, str2);
      strcat(sql, buffer);
      strcat(sql, str3);

      printf("\n%s\n", sql);
      
      //inserção 
      PQexec(conn, sql);
      // insert(sql);

   }
   PQexec(conn,"COMMIT");
   printf("END BEGIN\n");
   printf("Total de Linhas: %d\n", cont);
   printf("Tempo :%f \n",(clock() - tempo) / (double)CLOCKS_PER_SEC);

   
   return true;

}

char implicitTransaction(char buffer[], int buffer_size, FILE *arq, int cont){
   
   //monitorar o tempo de execução
   clock_t tempo;
   tempo = clock();
   while (fgets(buffer, buffer_size, arq))
   {
      cont++;
      char sql[buffer_size] = "INSERT INTO product(eid, description) VALUES (";
      char str2[buffer_size] = ", '";
      char str3[buffer_size] = "');";
      char szBuffer[1024];

      //printf("\n%d\n", cont);
      snprintf(szBuffer, sizeof (szBuffer), "%d",cont);
      strcat(sql, szBuffer);
      strcat(sql, str2);
      strcat(sql, buffer);
      strcat(sql, str3);

      printf("\n%s\n", sql);
      
      //inserção 
      PQexec(conn, sql);
      // insert(sql);

   }
   printf("Total de Linhas: %d\n", cont);
   printf("Tempo :%f \n",(clock() - tempo) / (double)CLOCKS_PER_SEC);

   
   return true;
 }

int mountSQL(){
   FILE *arq; 
   arq = fopen("data.csv", "r"); 
   int buffer_size = 1024;
   char buffer[1048];
   int cont = 25;


   // implicitTransaction(buffer, buffer_size, arq, cont);
   // explicitTransaction(buffer, buffer_size, arq, cont);
   // testInsert(buffer, buffer_size, arq, cont);
   insertWithErrosRollback();
   fclose(arq);
   return true;
}

int main()
{

   // /realiza a conexão/
   conn = PQconnectdb("dbname=TESTE user=vini password=1234");
    
   if(PQstatus(conn) == CONNECTION_OK)
   {
      printf("Conexão efetuada com sucesso. \n");

      //lendo arquivo csv
      mountSQL();
      //inserção provocando um rollback
      // insertWithErrosRollback();
   }
   else{
      printf("testando falha ");   
      printf("Falha na conexão. Erro: %s", PQerrorMessage(conn));
      PQfinish(conn);
      return -1;
   }
    
   //  /Verifica se a conexão está aberta e a encerra/
   if(conn != NULL)
      PQfinish(conn);
}