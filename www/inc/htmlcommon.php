<?php
// functions.php
// This file belongs to the Data2Pg web client. It describes general purpose functions.

// The PageHeader() function generates the HTML page header, including the Head, the page title and the main navigation bar.
function PageHeader() {
	global $conf;

	header('Content-Type: text/html');
	echo "<html>\n";
	// head
	echo "<head>\n";
	echo "\t<title>Data2Pg - a PostgreSQL Data Migration Framework</title>\n";
	echo "<meta charset=\"UTF-8\">";
	echo "\t<link href=\"css/data2pg.css\" rel=\"stylesheet\" type=\"text/css\">\n";
	echo "</head>\n";
	// body
	echo "<body>\n";
	echo "\t\t<header>\n";
	echo "\t\t\t<div class=\"headerLogo\">Data2Pg<span class=\"headerVersion\">" . $conf['version'] . "</span></div>\n";

	// Navigation links
	echo "\t\t\t<div class=\"headerButtons\">";
	echo "\t\t\t<a href=\"databases.php?a=display\" class=\"button headerButton\">Databases</a>\n";
	echo "\t\t\t<a href=\"runs.php?a=displayAllRuns\" class=\"button headerButton\">All runs</a>\n";
	echo "\t\t\t<a href=\"runs.php?a=displayInProgressRuns\" class=\"button headerButton\">In-progress runs</a>\n";
	echo "\t\t\t</div>\n";

	echo "\t\t\t<div class=\"headerTime\">" . date('Y-m-d') . "&nbsp;&nbsp;&nbsp;&nbsp;" . date('H:i:s') . "</div>\n";
	echo "\t\t</header>\n";
	echo "\t\t<main>\n";
}

// The () function build the title of the main section.
function mainTitle($leftContent = '', $centerContent = '', $rightContent = '') {
	echo "\t\t\t<div id=\"mainTitle\">\n";
	echo "\t\t\t\t<div id=\"mainTitleL\">\n" . $leftContent . "\n</div>\n";
	echo "\t\t\t\t<div id=\"mainTitleC\">\n" . $centerContent . "\n</div>\n";
	echo "\t\t\t\t<div id=\"mainTitleR\">\n" . $rightContent . "\n</div>\n";
	echo "\t\t\t</div>\n";
}

// The pageFooter() function generates the HTML page footer.
function PageFooter() {
	echo "\t\t</main>\n";
	echo "\t\t<footer>\n";

	echo "\t</footer>\n";
	echo "</body>\n";
	echo "</html>\n";
}

?>
