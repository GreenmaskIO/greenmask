package transformers

// ccTlds - countries domains
var ccTlds = []string{
	"us", // United States
	"uk", // United Kingdom (commonly .uk but officially .gb is reserved)
	"de", // Germany
	"ca", // Canada
	"fr", // France
	"au", // Australia
	"jp", // Japan
	"cn", // China
	"in", // India
	"br", // Brazil
	"ru", // Russia
	"za", // South Africa
	"nl", // Netherlands
	"mx", // Mexico
	"it", // Italy
	"es", // Spain
	"se", // Sweden
	"no", // Norway
	"fi", // Finland
	"dk", // Denmark
	"pl", // Poland
	"be", // Belgium
	"ch", // Switzerland
	"kr", // South Korea
	"sg", // Singapore
	"nz", // New Zealand
	"il", // Israel
	"ie", // Ireland
	"at", // Austria
	"pt", // Portugal
	"my", // Malaysia
	"th", // Thailand
	"ph", // Philippines
	"tr", // Turkey
	"id", // Indonesia
	"hk", // Hong Kong
	"ar", // Argentina
	"cl", // Chile
	"co", // Colombia
	"gr", // Greece
	"sa", // Saudi Arabia
	"ae", // United Arab Emirates
	"cy", // Cyprus
}

var gTlds = []string{
	"com",
	"org",
	"net",
	"int",
	"edu",
	"gov",
	"mil",
	"co",
	"tv",
	"xyz",
	"top",
	"club",
	"online",
	"site",
	"vip",
	"web",
	"info",
	"biz",
	"cc",
	"io",
}

// Predefined global variable containing a list of top email providers as a slice of strings
var defaultEmailProviders = []string{
	"gmail.com",      // Google Gmail
	"yahoo.com",      // Yahoo Mail
	"outlook.com",    // Microsoft Outlook
	"hotmail.com",    // Microsoft Hotmail (now part of Outlook)
	"aol.com",        // AOL Mail
	"icloud.com",     // Apple iCloud Mail
	"mail.com",       // Mail.com
	"zoho.com",       // Zoho Mail
	"yandex.com",     // Yandex Mail
	"protonmail.com", // ProtonMail
	"gmx.com",        // GMX Mail
	"fastmail.com",   // Fastmail
}
