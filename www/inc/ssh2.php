<?php
class Ssh2
{

	/** Ssh connection identifier */
	public $conn;

	/** SFTP SSHZ resource */
	public $sftp;

	/** Server Url */
	public $url;

	/** Connection user */
	public $user;

	/** Connection password */
	public $pwd;

	/** Connexion port */
	public $port;

	/** Sftp path */
	public $sftpPath;

	/**
	 *
	 * Constructor : perform the ssh and sftp connection.
	 *
	 * @param  string  Url  : Server Url
	 * @param  string  User : Connection user
	 * @param  string  Pwd  : Connection password
	 */
	public function __construct($url, $user, $pwd, $port='22')
	{
		$this->url  = $url;
		$this->user = $user;
		$this->pwd  = $pwd;
		$this->port = $port;

		$this->conn = ssh2_connect($this->url, $this->port);
		if(!$this->conn)
		{
			throw new Exception("Log on to the SFTP server " . $this->url . " and port " . $this->port . " failed");
		}

		if (! ssh2_auth_password($this->conn, $this->user, $this->pwd))
		{
			throw new Exception("Authentication with user " . $this->user . " and password " .  $this->pwd . " failed");
		}
		$this->sftp = ssh2_sftp($this->conn);
		if (! $this->sftp)
		{
			throw new Exception("SFTP initialization failed");
		}
		$this->sftpPath="ssh2.sftp://" .$this->Sftp;
	}
	/**
	 *
	 * Destructor : close the connections normally
	 *
	**/
	public function __destruct() {
//        echo "Destroy " . get_class($this) . "\n";
		$this->conn=null;
		$this->sftp=null;
	}
	/**
	 *
	 * execCmd : Send a command
	 *
	 * @param  string  command : The command to execute
	 * @param  string  async   : if not empty, means that the command has to be send in asynchronous mode
	 */
	public function execCmd($command, $async="")
	{
		if(!($stream = ssh2_exec($this->conn, $command)))
		{
			throw new Exception("Problem with the command: $command");
		}
		if ($async <> "") {
			return $stream;
		} else {
			stream_set_blocking($stream, true);
			$data = "";
			while ($buf = fread($stream, 4096)) {
				$data .= $buf;
			}
			fclose($stream);
			return $data;
		}
	}
}
?>