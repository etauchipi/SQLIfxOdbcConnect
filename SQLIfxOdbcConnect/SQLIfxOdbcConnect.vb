Imports System.Data
Imports System.Data.Odbc

Public Class SQLOdbcOdbcConnect
    ' -------------------------------------------------
    ' Function OpenData
    '
    ' Realiza la inicialización de opbjetos de un DataReader
    '
    ' Valor Devuelto:
    ' - Boolean
    '
    ' Parámetros: 
    ' - sCnn
    ' - sCommand
    ' - Connection
    ' - command
    ' - reader
    ' Se crea una nueva instancia SqlConnection en Connection con la cadena de conexión sCnn
    ' Luego, se crea una instancia de SqlCommand en command, con la cadena de consulta sCommand y la conexión Connection
    ' Se intenta abrir la conexión y luego la consulta
    ' En caso de fallo, se emite un beep y devuelve un valor False
    ''' <summary>
    ''' Ejecuta el comando Open de un objeto Connection que se inicializa con una cadena de conexión pasada como parámetro
    ''' </summary>
    ''' <param name="sCnn">Cadena de conexión a la base de datos</param>
    ''' <param name="sCommand">Cadena con la especificación del query a ejecutar pasado como referencia</param>
    ''' <param name="Connection">Objeto tipo SqlConnection pasado como referencia</param>
    ''' <param name="command">Objeto tipo SqlCommand pasado como referencia</param>
    ''' <returns>
    ''' Valor tipo boolean con el resultado de la operación Open
    ''' </returns>
    Public Function OpenData(ByVal sCnn As String, ByVal sCommand As String, ByRef Connection As OdbcConnection, ByRef command As OdbcCommand) As Boolean

        Dim Retorno As Boolean

        Retorno = True
        Connection = New OdbcConnection(sCnn)
        command = New OdbcCommand(sCommand, Connection)
        command.CommandTimeout = wsCargaTipo.wsNormal

        Try
            Connection.Open()
        Catch ex As Exception    ' Captura el error.
            Retorno = False

        Finally
            ' Código a ejecutar siempre.
        End Try

        Return Retorno

    End Function

    Public Async Function OpenDataAsync(ByVal sCnn As String, ByVal sCommand As String, ByVal Connection As OdbcConnection, ByVal command As OdbcCommand) As Task(Of Boolean)

        Dim Retorno As Boolean

        Retorno = True
        Connection = New OdbcConnection(sCnn)
        command = New OdbcCommand(sCommand, Connection)
        command.CommandTimeout = wsCargaTipo.wsNormal

        Try
            Await Connection.OpenAsync()
            Retorno = True
        Catch ex As Exception    ' Captura el error.
            Retorno = False

        Finally
            ' Código a ejecutar siempre.
        End Try

        Return Retorno

    End Function


    ' Function OpenData

    ' -------------------------------------------------

    ' -------------------------------------------------
    ' Sub CloseConn
    '
    ' Realiza el cierre de una conexión sqlConnection
    '
    ' Valor Devuelto:
    ' Ninguno
    '
    ' Parámetros: 
    ' - Connection
    ' - command
    ' Verifica el estado de una conexión sqlConnection y lo cierra
    ''' <summary>
    ''' ejecutas el comando Close de una conexión específica
    ''' </summary>
    ''' <param name="Connection">Referencia al objeto tipo SqlConnection que se desea cerrar</param>
    ''' <returns>
    ''' Indicador de exito de la operación
    ''' </returns>
    Public Function CloseConn(ByRef Connection As OdbcConnection) As Boolean

        Dim Retorno As Boolean

        Retorno = True

        Try
            If Connection.State <> ConnectionState.Closed Then
                Connection.Close()
            End If
        Catch ex As Exception
            Retorno = False
        Finally
        End Try

        Return Retorno

    End Function
    ' Sub CloseConn
    ' -------------------------------------------------

    ''' <summary>
    ''' Carga información resultante del query realizado según parámetro
    ''' </summary>
    ''' <param name="sCnn">Cadena con el query a ejecutar. Debe contener "AS Valor" dentro de la especificación del query como requisito para recuperar el valor buscado</param>
    ''' <param name="sCommand">Cadena de comando</param>
    ''' <param name="nCarga">Id del tipo de carga que tendrá el acceso a la BD</param>
    ''' <returns>
    ''' Estructura con resultado del query
    ''' </returns>
    Public Function GetDataDb_Reader(ByVal sCnn As String, ByVal sCommand As String, Optional ByVal nCarga As wsCargaTipo = wsCargaTipo.wsNormal) As OdbcDataReader

        Dim Retorno As OdbcDataReader
        Dim Connection As OdbcConnection
        Dim command As OdbcCommand

        Connection = New OdbcConnection(sCnn)
        command = New OdbcCommand(sCommand, Connection)
        command.CommandTimeout = nCarga
        Retorno = Nothing

        If OpenData(sCnn, sCommand, Connection, command) Then

            Using Connection

                Try
                    Retorno = command.ExecuteReader()
                Catch ex As Exception
                    Retorno = Nothing
                Finally
                End Try

            End Using

        End If

        Return Retorno

    End Function

    Public Async Function GetDataDb_ReaderAsync(ByVal sCnn As String, ByVal sCommand As String, Optional ByVal nCarga As wsCargaTipo = wsCargaTipo.wsNormal) As Task(Of OdbcDataReader)

        Dim Retorno As OdbcDataReader
        Dim Connection As OdbcConnection
        Dim command As OdbcCommand

        Connection = New OdbcConnection(sCnn)
        command = New OdbcCommand(sCommand, Connection)
        command.CommandTimeout = nCarga
        Retorno = Nothing

        If OpenData(sCnn, sCommand, Connection, command) Then

            Using Connection

                Try
                    Retorno = Await command.ExecuteReaderAsync()
                Catch ex As Exception
                    Retorno = Nothing
                Finally
                End Try

            End Using

        End If

        Return Retorno

    End Function

    ' -------------------------------------------------

    ' -------------------------------------------------
    ' Sub ejecutarSQL
    '
    ' Realiza una ejecución SQL que no devuelve un valor
    '
    ' Valor Devuelto:
    ' Ninguno
    '
    ' Parámetros: 
    ' - sCnn
    ' - cConn
    ' Verifica el estado de una conexión sqlConnection y lo cierra
    ''' <summary>
    ''' Realiza la ejecución de un query (utilizando una transacción)
    ''' </summary>
    ''' <param name="sCnn">Cadena especificando el query a ejecutar</param>
    ''' <param name="cConn">Cadena de conexión a la base de datos</param>
    ''' <param name="nCarga">Id del tipo de carga que tendrá el acceso a la BD</param>
    ''' <param name="IsolationTipo">Isolation type para el acceso a la BD</param>
    ''' <returns>
    ''' Indicador de exito del query
    ''' </returns>
    ''' <exception cref="System.Exception">Error ejecutando el query</exception>
    Public Function EjecutarSQL(ByVal sCnn As String, ByVal cConn As String, Optional ByVal nCarga As wsCargaTipo = wsCargaTipo.wsNormal, Optional ByVal IsolationTipo As IsolationLevel = IsolationLevel.Serializable) As Boolean

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Boolean
        Dim tTransaction As OdbcTransaction

        Retorno = True
        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = nCarga

        If OpenData(sCnn, cConn, Connection, Command) Then

            tTransaction = Connection.BeginTransaction(IsolationTipo)

            Using Connection

                Try
                    Command.Transaction = tTransaction
                    reader = Command.ExecuteReader()
                    reader.Close()
                    Command.Transaction.Commit()
                Catch ex As Exception
                    Command.Transaction.Rollback()
                    Retorno = False
                    Throw New System.Exception("Error ejecutando el query")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    Public Async Function EjecutarSQLAsync(ByVal sCnn As String, ByVal cConn As String, Optional ByVal nCarga As wsCargaTipo = wsCargaTipo.wsNormal, Optional ByVal IsolationTipo As IsolationLevel = IsolationLevel.Serializable) As Task(Of Boolean)

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Boolean
        Dim tTransaction As OdbcTransaction

        Retorno = True
        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = nCarga

        If OpenData(sCnn, cConn, Connection, Command) Then

            tTransaction = Connection.BeginTransaction(IsolationTipo)

            Using Connection

                Try
                    Command.Transaction = tTransaction
                    reader = Await Command.ExecuteReaderAsync()
                    reader.Close()
                    Command.Transaction.Commit()
                Catch ex As Exception
                    Command.Transaction.Rollback()
                    Retorno = False
                    Throw New System.Exception("Error ejecutando el query")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    ' -------------------------------------------------

    ''' <summary>
    ''' Carga información resultante del query realizado según parámetro
    ''' </summary>
    ''' <param name="sCnn">Cadena con el query a ejecutar. Debe contener "AS Valor" dentro de la especificación del query como requisito para recuperar el valor buscado</param>
    ''' <param name="cConn">Cadena de conexión a la base de datos</param>
    ''' <param name="nCarga">Id del tipo de carga que tendrá el acceso a la BD</param>
    ''' <returns>
    ''' Estructura con resultado del query
    ''' </returns>
    Public Function GetDataDb(sCnn As String, cConn As String, Optional ByVal nCarga As wsCargaTipo = wsCargaTipo.wsNormal) As DataSet

        Dim Retorno As DataSet
        Dim da As OdbcDataAdapter
        Dim Connection As OdbcConnection

        'Cambiar el conector en cada ambiente
        Connection = New OdbcConnection(sCnn)
        da = New OdbcDataAdapter
        Retorno = New DataSet


        da = New OdbcDataAdapter(cConn, Connection)
        da.Fill(Retorno, "Table1")
        CloseConn(Connection)

        Return Retorno

    End Function
    ' -------------------------------------------------

    ' -------------------------------------------------
    ' Sub CargarValor
    '
    ' Realiza una ejecución SQL que devuelve un valor Decimal
    '
    ' Valor Devuelto:
    ' Ninguno
    '
    ' Parámetros: 
    ' - sCnn
    ' - cConn
    ' Verifica el estado de una conexión sqlConnection y lo cierra
    ''' <summary>
    ''' Recupera un valor con la información solicitada en el query pasado<br></br>
    ''' la condición para utilizar CargaValor es que el query especifique el valor devuelto como "AS Valor", porque es el elemento que se devuelve por la función
    ''' </summary>
    ''' <param name="sCnn">Cadena con el query a ejecutar. Debe contener "AS Valor" dentro de la especificación del query como requisito para recuperar el valor buscado</param>
    ''' <param name="cConn">Cadena de conexión a la base de datos</param>
    ''' <returns>
    ''' Variable de nombre "Valor" de tipo decimal, con el valor resultante de la consulta
    ''' </returns>
    Public Function CargarValor(ByVal sCnn As String, ByVal cConn As String) As Decimal

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Decimal

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal
        Retorno = 0.0

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Command.ExecuteReader()
                    reader.Read()
                    Retorno = CType(reader.Item("Valor"), Decimal)
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga")
                Finally
                    '
                End Try

            End Using

        End If

        Return Retorno

    End Function

    Public Async Function CargarValorAsync(ByVal sCnn As String, ByVal cConn As String) As Task(Of Decimal)

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Decimal

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal
        Retorno = 0.0

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Await Command.ExecuteReaderAsync()
                    reader.Read()
                    Retorno = CType(reader.Item("Valor"), Decimal)
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga")
                Finally
                    '
                End Try

            End Using

        End If

        Return Retorno

    End Function

    ' -------------------------------------------------

    ''' <summary>
    ''' Recupera un valor con la información solicitada en el query pasado<br></br>
    ''' la condición para utilizar CargarId es que el query especifique el valor devuelto como "AS Valor", porque es el elemento que se devuelve por la función
    ''' </summary>
    ''' <param name="sCnn">Cadena con el query a ejecutar. Debe contener "AS Valor" dentro de la especificación del query como requisito para recuperar el valor buscado</param>
    ''' <param name="cConn">Cadena de conexión a la base de datos</param>
    ''' <returns>
    ''' Variable de nombre "Valor" de tipo integer, con el valor resultante de la consulta
    ''' </returns>
    Public Function CargarId(ByVal sCnn As String, ByVal cConn As String) As Integer

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Integer

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal

        Retorno = 0

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Command.ExecuteReader()
                    reader.Read()
                    Retorno = CType(reader.Item("Valor"), Integer)
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    Public Async Function CargarIdAsync(ByVal sCnn As String, ByVal cConn As String) As Task(Of Integer)

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As Integer

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal

        Retorno = 0

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Await Command.ExecuteReaderAsync()
                    reader.Read()
                    Retorno = CType(reader.Item("Valor"), Integer)
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    ' -------------------------------------------------

    ''' <summary>
    ''' Recupera una cadenacon la información solicitada en el query pasado<br></br>
    ''' la condición para utilizar CargarCadena es que el query especifique el valor devuelto como "AS Valor", porque es el elemento que se devuelve por la función
    ''' </summary>
    ''' <param name="sCnn">Cadena con el query a ejecutar. Debe contener "AS Valor" dentro de la especificación del query como requisito para recuperar el valor buscado</param>
    ''' <param name="cConn">Cadena de conexión a la base de datos</param>
    ''' <returns>
    ''' Variable de nombre "Valor" de tipo string, con el valor resultante de la consulta
    ''' </returns>
    Public Function CargarCadena(ByVal sCnn As String, ByVal cConn As String) As String

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As String

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal

        Retorno = String.Empty

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Command.ExecuteReader()
                    reader.Read()
                    Retorno = Trim(reader.Item("Valor"))
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga de cadena")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    Public Async Function CargarCadenaAsync(ByVal sCnn As String, ByVal cConn As String) As Task(Of String)

        Dim reader As OdbcDataReader
        Dim Command As New OdbcCommand
        Dim Connection As New OdbcConnection
        Dim Retorno As String

        Connection = New OdbcConnection(sCnn)
        Command.CommandTimeout = wsCargaTipo.wsNormal

        Retorno = String.Empty

        If OpenData(sCnn, cConn, Connection, Command) Then

            Using Connection

                Try
                    reader = Await Command.ExecuteReaderAsync()
                    reader.Read()
                    Retorno = Trim(reader.Item("Valor"))
                    reader.Close()

                Catch ex As Exception
                    Throw New System.Exception("Error ejecutando el query de carga de cadena")
                Finally
                    '
                End Try

            End Using

        End If

        CloseConn(Connection)

        Return Retorno

    End Function

    ' -------------------------------------------------

End Class

''' <summary>
''' Tipo de carga a realizarse en un acceso a la base de datos
''' </summary>
Public Enum wsCargaTipo
    wsNormal = 300
    wsMedio = 600
    wsGrande = 1200
End Enum
