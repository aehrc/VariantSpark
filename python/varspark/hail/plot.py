from hail.plot.plots import *
from hail.plot.plots import _collect_scatter_plot_data, _get_scatter_plot_elements


@typecheck(pvals=expr_float64, locus=nullable(expr_locus()), title=nullable(str),
           size=int, hover_fields=nullable(dictof(str, expr_any)), collect_all=bool,
           n_divisions=int, significance_line=nullable(numeric))
def manhattan_imp(pvals, locus=None, title=None, size=4, hover_fields=None, collect_all=False,
                  n_divisions=500, significance_line=5e-8):
    """Create a Manhattan plot. (https://en.wikipedia.org/wiki/Manhattan_plot)

    Parameters
    ----------
    pvals : :class:`.Float64Expression`
        P-values to be plotted.
    locus : :class:`.LocusExpression`
        Locus values to be plotted.
    title : str
        Title of the plot.
    size : int
        Size of markers in screen space units.
    hover_fields : Dict[str, :class:`.Expression`]
        Dictionary of field names and values to be shown in the HoverTool of the plot.
    collect_all : bool
        Whether to collect all values or downsample before plotting.
    n_divisions : int
        Factor by which to downsample (default value = 500). A lower input results in fewer output datapoints.
    significance_line : float, optional
        p-value at which to add a horizontal, dotted red line indicating
        genome-wide significance.  If ``None``, no line is added.

    Returns
    -------
    :class:`bokeh.plotting.figure.Figure`
    """
    if locus is None:
        locus = pvals._indices.source.locus

    ref = locus.dtype.reference_genome

    if hover_fields is None:
        hover_fields = {}

    hover_fields['locus'] = hail.str(locus)

    # pvals = -hail.log10(pvals)

    source_pd = _collect_scatter_plot_data(
        ('_global_locus', locus.global_position()),
        ('_pval', pvals),
        fields=hover_fields,
        n_divisions=None if collect_all else n_divisions
    )
    source_pd['p_value'] = list(source_pd['_pval'])
    source_pd['_contig'] = [locus.split(":")[0] for locus in source_pd['locus']]

    observed_contigs = set(source_pd['_contig'])
    observed_contigs = [contig for contig in ref.contigs.copy() if contig in observed_contigs]
    contig_ticks = hail.eval(
        [hail.locus(contig, int(ref.lengths[contig] / 2)).global_position() for contig in
         observed_contigs])
    color_mapper = CategoricalColorMapper(factors=ref.contigs,
                                          palette=palette[:2] * int((len(ref.contigs) + 1) / 2))

    p = figure(title=title, x_axis_label='Chromosome', y_axis_label='Importance (gini)', width=1000)
    p, _, legend, _, _, _ = _get_scatter_plot_elements(
        p, source_pd, x_col='_global_locus', y_col='_pval',
        label_cols=['_contig'], colors={'_contig': color_mapper},
        size=size
    )
    legend.visible = False
    p.xaxis.ticker = contig_ticks
    p.xaxis.major_label_overrides = dict(zip(contig_ticks, observed_contigs))
    p.select_one(HoverTool).tooltips = [t for t in p.select_one(HoverTool).tooltips if
                                        not t[0].startswith('_')]

    if significance_line is not None:
        p.renderers.append(Span(location=-math.log10(significance_line),
                                dimension='width',
                                line_color='red',
                                line_dash='dashed',
                                line_width=1.5))

    return p
