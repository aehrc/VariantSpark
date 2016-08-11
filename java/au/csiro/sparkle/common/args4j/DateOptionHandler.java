package au.csiro.sparkle.common.args4j;

import java.util.Date;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OneArgumentOptionHandler;
import org.kohsuke.args4j.spi.Setter;


public class DateOptionHandler extends OneArgumentOptionHandler<Date> {

	private static final DateTimeFormatter INPUT_DATE_FORMAT = DateTimeFormat.forPattern("YYYY-MM-dd_HH:mm");
	
    public DateOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Date> setter) {
        super(parser, option, setter);
    }
	
	@Override
	public String getDefaultMetaVariable() {
		// TODO Auto-generated method stub
		return "DATE";
	}

	@Override
	protected Date parse(String arg0) throws CmdLineException {
		try {
			return INPUT_DATE_FORMAT.parseDateTime(arg0).toDate();
		} catch (IllegalArgumentException ex) {
			throw new CmdLineException(ex.getMessage());
		}
	}
}
