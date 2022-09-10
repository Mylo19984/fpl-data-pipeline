from application import db

class FplPlayerData(db.Model):
    __tablename__ = 'player_general'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(30))
    surname = db.Column(db.String(30))
    form = db.Column(db.Numeric(10,2))
    total_points = db.Column(db.Integer)
    now_costs = db.Column(db.Numeric(10,2))

    __table_args__ = {'schema': 'mylo'}

    def __str__(self):
        return self.id

