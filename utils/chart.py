"""Chart helper functions"""
import altair as alt

# show chart that displays evolution of APR%


def get_apr_chart(data):
    hover = alt.selection_single(
        fields=["created_at"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(data, title="Evolution of awards.")
        .mark_line()
        .encode(
            x="created_at",
            y="apr_total",
            color="symbol",
            strokeDash="symbol",
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x=alt.X("created_at", axis=alt.Axis(title='Date')),
            y=alt.Y("apr_total", axis=alt.Axis(format='%', title='APR (%)')),
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("created_at", title="Date"),
                alt.Tooltip("apr_total", format='%', title="APR (%)"),
            ],
        )
        .add_selection(hover)
    )

    return (lines + points + tooltips).interactive()

# show chart that displays current holdings


def get_holdings_chart(data):
    bars = alt.Chart(data).mark_bar().encode(
        x='Amount (USD):Q',
        y=alt.Y("Coin:O", sort='-x')
    )

    return (bars).properties(height=300)

# show chart that displays timeseries of token holdings


def get_historical_holdings_chart(data):
    hover = alt.selection_single(
        fields=["created_at"],
        nearest=True,
        on="mouseover",
        empty="none",
    )

    lines = (
        alt.Chart(data, title="Evolution of holdings against current prices.")
        .mark_line()
        .encode(
            x="created_at",
            y="Amount (USD)",
            color="Coin",
            strokeDash="Coin",
        )
    )

    # Draw points on the line, and highlight based on selection
    points = lines.transform_filter(hover).mark_circle(size=65)

    # Draw a rule at the location of the selection
    tooltips = (
        alt.Chart(data)
        .mark_rule()
        .encode(
            x=alt.X("created_at", axis=alt.Axis(title='Date')),
            y=alt.Y("Amount (USD)", axis=alt.Axis(title='Amount (USD)')),
            opacity=alt.condition(hover, alt.value(0.3), alt.value(0)),
            tooltip=[
                alt.Tooltip("created_at", title="Date"),
                alt.Tooltip("Amount (USD)", format="$", title="Amount (USD)"),
            ],
        )
        .add_selection(hover)
    )

    return (lines + points + tooltips).interactive()
