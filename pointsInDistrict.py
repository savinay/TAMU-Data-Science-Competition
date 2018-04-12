# pylint: skip-file
# Vertices of Polygon
x1 = (-87.668046, 41.925163)
x2 = (-87.632791, 41.925714)
x3 = (-87.613650, 41.892051)
x4 = (-87.611171, 41.852618)
x5 = (-87.660700, 41.851470)


ABOVE = "above"
BELOW = "below"
RIGHT = "right"
LEFT = "left"


def slopeint(a, b):
    """Function to Calculate Slope and Intercept of 2 Points"""
    slope = (a[1] - b[1]) / (a[0] - b[0])
    intercept = ((-1) * a[0] * slope) + a[1]
    return slope, intercept


def position(p, slope, intercept):
    y = BELOW if intercept + slope * p[0] > p[1] else ABOVE
    x = LEFT if (p[1] - intercept) / slope > p[0] else RIGHT
    return (x, y)


def insidePentagon(p, bounds):
    inside = [(RIGHT, BELOW), (LEFT, BELOW), (LEFT, BELOW),
              (LEFT, ABOVE), (RIGHT, ABOVE)]
    slopeints = [slopeint(a, b) for a, b in zip(
        bounds,  bounds[1:] + [bounds[0]])]
    return [position(p, s, i) for s, i in slopeints] == inside


p = (-87.64, 41.88)
print(insidePentagon(p, [x1, x2, x3, x4, x5]))
