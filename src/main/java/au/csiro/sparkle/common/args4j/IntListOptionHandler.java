package au.csiro.sparkle.common.args4j;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.DelimitedOptionHandler;
import org.kohsuke.args4j.spi.IntOptionHandler;
import org.kohsuke.args4j.spi.Setter;


public class IntListOptionHandler extends DelimitedOptionHandler<Integer> {

    public IntListOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Integer> setter) {
        super(parser, option, setter, ",", new IntOptionHandler(parser, option, setter));
    }
}
