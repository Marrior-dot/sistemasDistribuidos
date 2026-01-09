
$coordenador = 'false'

$c = New-Object System.Net.Sockets.TcpClient("localhost", 3000); $s = $c.GetStream(); $w = New-Object System.IO.StreamWriter($s); $w.WriteLine($coordenador);$w.WriteLine("MENSAGEM_TESTE"); $w.Flush(); $c.Close()
